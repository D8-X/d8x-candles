package polyclient

import (
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type PolyClient struct {
	AssetIds   map[string]string //dec id -> hex id
	MuAssets   sync.Mutex
	MuWs       sync.Mutex
	Ws         *websocket.Conn
	reconnChan chan struct{}
}

func NewPolyClient() *PolyClient {
	pc := PolyClient{
		AssetIds:   make(map[string]string),
		MuAssets:   sync.Mutex{},
		MuWs:       sync.Mutex{},
		Ws:         nil,
		reconnChan: make(chan struct{}),
	}
	return &pc
}

// SubscribeAssetIds adds an asset id and subscribes
func (pc *PolyClient) SubscribeAssetIds(ids []string) error {
	pc.MuAssets.Lock()
	defer pc.MuAssets.Unlock()
	for _, id := range ids {
		if _, exists := pc.AssetIds[id]; exists {
			continue
		}
		hx, err := utils.Dec2Hex(id)
		if err != nil || len(hx) != 66 {
			return fmt.Errorf("invalid id %s, %v", id, err)
		}
		pc.AssetIds[id] = hx
	}
	pc.MuWs.Lock()
	if pc.Ws == nil {
		slog.Info("ws not ready yet")
		pc.MuWs.Unlock()
		return nil
	}
	pc.MuWs.Unlock()

	// to subscribe we need to reconnect
	close(pc.reconnChan)
	pc.reconnChan = make(chan struct{})
	return nil
}

func (pc *PolyClient) ReSubscribe() error {
	pc.MuAssets.Lock()
	defer pc.MuAssets.Unlock()
	assetIds := make([]string, 0, len(pc.AssetIds))
	for idDec := range pc.AssetIds {
		assetIds = append(assetIds, idDec)
	}
	return pc.subscribe(assetIds)
}

func GetMarketInfo(conditionId string) (*utils.PolyMarketInfo, error) {
	endpt := "https://clob.polymarket.com/markets/" + conditionId
	resp, err := http.Get(endpt)
	if err != nil {
		return nil, fmt.Errorf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var marketInfo utils.PolyMarketInfo
	err = json.Unmarshal(body, &marketInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	return &marketInfo, nil
}

func (pc *PolyClient) RunWs(stop chan struct{}) {
	for {
		err := pc.ConnectWs(stop)
		if err != nil {
			fmt.Println("Reconnecting WS, reason:", err)
			time.Sleep(1 * time.Second) // Reconnect after a delay
		} else {
			fmt.Println("Connection closed gracefully")
			return
		}
	}
}

// ConnectWs connects and listens to the websocket, terminates on error
// asset Ids are provided in decimal format string
func (pc *PolyClient) ConnectWs(stop chan struct{}) error {

	url := "wss://ws-subscriptions-clob.polymarket.com/ws/market"

	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("dial: %s", err.Error())
	}
	defer c.Close()

	pc.MuWs.Lock()
	pc.Ws = c
	pc.MuWs.Unlock()

	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			err = pc.handleEvent(string(message))
			if err != nil {
				fmt.Printf("error handling event: %v", err)
			}
		}
	}()

	if len(pc.AssetIds) > 0 {
		fmt.Printf("resubscribing to %d assets\n", len(pc.AssetIds))
		pc.ReSubscribe()
	}

	for {
		select {
		case err := <-errCh:
			fmt.Println("WebSocket connection error")
			return err
		case <-done:
			fmt.Println("WebSocket connection closed")
			return nil
		case <-stop:
			fmt.Println("stopping WebSocket connection")
			return nil
		case <-pc.reconnChan:
			return fmt.Errorf("reconnect for new assets required")
		}
	}
}

// subscribe sends a subscription message to the websocket
// connection; assetIds are in decimal format
func (pc *PolyClient) subscribe(assetIds []string) error {

	subscribeMessage := map[string]interface{}{
		"type":       "Market",
		"assets_ids": assetIds,
	}

	message, err := json.Marshal(subscribeMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal subscribe message: %w", err)
	}
	pc.MuWs.Lock()
	defer pc.MuWs.Unlock()
	if pc.Ws == nil {
		slog.Info("ws not ready yet")
		return nil
	}
	fmt.Printf("Sending subscribe message: %s\n", message)
	err = pc.Ws.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		return fmt.Errorf("failed to write subscribe message: %w", err)
	}
	fmt.Println("Subscribe message sent successfully")
	return nil
}

// handleEvent parses ws-subscriptions-clob.polymarket events
func (pc *PolyClient) handleEvent(eventJson string) error {
	if eventJson == "Invalid command" {
		return fmt.Errorf(eventJson)
	}
	// Process price change event
	var eventMap map[string]interface{}
	err := json.Unmarshal([]byte(eventJson), &eventMap)
	if err != nil {
		return fmt.Errorf("error unmarshalling JSON: %v, message=%s", err, eventJson)
	}

	eventType, ok := eventMap["event_type"].(string)
	if !ok {
		return fmt.Errorf("event_type not found or not a string: %s", eventJson)
	}
	id := eventMap["asset_id"].(string)
	slog.Info(fmt.Sprintf("polyclient received event '%s' for %s", eventType, pc.AssetIds[id]))
	if eventType == "book" {
		return nil
	}
	var priceChange utils.PolyPriceChange
	err = json.Unmarshal([]byte(eventJson), &priceChange)
	if err != nil {
		return fmt.Errorf("error unmarshalling price change JSON: %v", err)
	}
	fmt.Printf("asset=%s price = %s\n", pc.AssetIds[priceChange.AssetID], priceChange.Price)
	RestQueryPrice(priceChange.AssetID)
	return nil
}

// restQueryPrice queries the mid-price for the given token id (decimal) from
// polymarket rest API
func RestQueryPrice(tokenId string) error {
	url := "https://clob.polymarket.com/midpoint?token_id=" + tokenId
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	var px utils.PolyMidPrice
	err = json.Unmarshal(body, &px)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %v", err)
	}
	fmt.Printf("mid price = %.2f\n", px.Px)
	return nil
}
