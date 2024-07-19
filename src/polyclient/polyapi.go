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

// PolyApi handles the connections to polymarket
// websocket and REST API
type PolyApi struct {
	AssetIds   map[string]string //dec id -> symbol
	MuAssets   sync.Mutex
	MuWs       sync.Mutex
	Ws         *websocket.Conn
	reconnChan chan struct{}
	apiBucket  *utils.TokenBucket
}

func NewPolyApi() *PolyApi {

	pa := PolyApi{
		AssetIds:   make(map[string]string),
		MuAssets:   sync.Mutex{},
		MuWs:       sync.Mutex{},
		Ws:         nil,
		reconnChan: make(chan struct{}),
		apiBucket:  utils.NewTokenBucket(4, 4.0),
	}
	return &pa
}

// SubscribeAssetIds adds asset ids and restarts
// websocket for subscription if needed
func (pa *PolyApi) SubscribeAssetIds(ids, sym []string) error {
	pa.MuAssets.Lock()
	defer pa.MuAssets.Unlock()
	newAssets := 0
	for k, id := range ids {
		if _, exists := pa.AssetIds[id]; exists {
			continue
		}
		newAssets++
		hx, err := utils.Dec2Hex(id)
		if err != nil || len(hx) != 66 {
			return fmt.Errorf("invalid id %s, %v", id, err)
		}
		pa.AssetIds[id] = sym[k]
	}
	if newAssets == 0 {
		slog.Info("no new asset in subscription")
		return nil
	}

	pa.MuWs.Lock()
	if pa.Ws == nil {
		slog.Info("ws not ready yet")
		pa.MuWs.Unlock()
		return nil
	}
	pa.MuWs.Unlock()

	// to subscribe we need to reconnect
	close(pa.reconnChan)
	pa.reconnChan = make(chan struct{})
	return nil
}

// ReSubscribe subscribes to all assets in the polyApi mapping
func (pa *PolyApi) ReSubscribe() error {
	pa.MuAssets.Lock()
	defer pa.MuAssets.Unlock()
	assetIds := make([]string, 0, len(pa.AssetIds))
	for idDec := range pa.AssetIds {
		assetIds = append(assetIds, idDec)
	}
	return pa.sendSubscribe(assetIds)
}

func GetMarketInfo(bucket *utils.TokenBucket, conditionId string) (*utils.PolyMarketInfo, error) {
	endpt := "https://clob.polymarket.com/markets/" + conditionId
	bucket.WaitForToken(time.Millisecond*250, "get market info")
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

// RunWs keeps the websocket connection alive.
func (pa *PolyApi) RunWs(stop chan struct{}, pc *PolyClient) {

	for {
		timeStart := time.Now()
		err := pa.ConnectWs(stop, pc)
		if err != nil {
			slog.Info(fmt.Sprintf("Reconnecting WS, after %s reason: %v\n", time.Since(timeStart), err))
			sleep := 1 + max(0, 5-time.Since(timeStart))
			time.Sleep(sleep * time.Second) // Reconnect after a delay
		} else {
			fmt.Println("Connection closed gracefully")
			return
		}
	}
}

// ConnectWs connects and listens to the websocket, terminates on error
// asset Ids are provided in decimal format string
func (pa *PolyApi) ConnectWs(stop chan struct{}, pc *PolyClient) error {

	url := "wss://ws-subscriptions-clob.polymarket.com/ws/market"

	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("dial: %s", err.Error())
	}
	defer c.Close()

	pa.MuWs.Lock()
	pa.Ws = c
	pa.MuWs.Unlock()

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
			err = pa.handleEvent(string(message), pc)
			if err != nil {
				fmt.Printf("error handling event: %v", err)
			}
		}
	}()

	if len(pa.AssetIds) > 0 {
		fmt.Printf("subscribing to %d assets\n", len(pa.AssetIds))
		pa.ReSubscribe()
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
		case <-pa.reconnChan:
			return fmt.Errorf("reconnect for new assets required")
		}
	}
}

// sendSubscribe sends a subscription message to the websocket
// connection; assetIds are in decimal format
func (pa *PolyApi) sendSubscribe(assetIds []string) error {

	subscribeMessage := map[string]interface{}{
		"type":       "Market",
		"assets_ids": assetIds,
	}

	message, err := json.Marshal(subscribeMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal subscribe message: %w", err)
	}
	pa.MuWs.Lock()
	defer pa.MuWs.Unlock()
	if pa.Ws == nil {
		slog.Info("ws not ready yet")
		return nil
	}
	fmt.Printf("Sending subscribe message: %s\n", message)
	err = pa.Ws.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		return fmt.Errorf("failed to write subscribe message: %w", err)
	}
	return nil
}

// handleEvent parses ws-subscriptions-clob.polymarket events
func (pa *PolyApi) handleEvent(eventJson string, pc *PolyClient) error {
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
	slog.Info(fmt.Sprintf("polyclient received event '%s' for %s", eventType, pa.AssetIds[id]))
	if eventType == "book" {
		return nil
	}
	var priceChange utils.PolyPriceChange
	err = json.Unmarshal([]byte(eventJson), &priceChange)
	if err != nil {
		return fmt.Errorf("error unmarshalling price change JSON: %v", err)
	}
	fmt.Printf("asset=%s price = %s\n", pa.AssetIds[priceChange.AssetID], priceChange.Price)
	px, err := RestQueryPrice(pa.apiBucket, priceChange.AssetID)
	if err != nil {
		return fmt.Errorf("error querying price: %v", err)
	}
	if pc != nil {
		// update redis
		pc.OnNewPrice(pa.AssetIds[priceChange.AssetID], px, int64(priceChange.TimestampMs))
	}
	return nil
}

func (pa *PolyApi) FetchMktHours(conditionIds []string) []utils.MarketHours {
	return fetchMktHours(pa.apiBucket, conditionIds)
}

func fetchMktHours(bucket *utils.TokenBucket, conditionIds []string) []utils.MarketHours {
	mkts := make([]utils.MarketHours, 0, len(conditionIds))
	for _, id := range conditionIds {
		m, err := GetMarketInfo(bucket, id)
		if err != nil {
			slog.Info(fmt.Sprintf("FetchMktInfo: id %s: %v", id, err))
			continue
		}
		var endDateTs int64
		endDate, err := time.Parse(time.RFC3339, m.EndDateISO)
		if err != nil {
			slog.Error(fmt.Sprintf("FetchMktInfo: parsing time for condition id %s: %v", id, err))
		} else {
			endDateTs = endDate.Unix()
		}
		nowTs := time.Now().Unix()
		isOpen := m.Active && !m.Closed && m.AcceptingOrders && (endDateTs == 0 || endDateTs > nowTs)
		hrs := utils.MarketHours{
			IsOpen:    isOpen,
			NextOpen:  0,
			NextClose: endDateTs,
		}
		mkts = append(mkts, hrs)
	}
	return mkts
}

// restQueryPrice queries the mid-price for the given token id (decimal) from
// polymarket rest API
func RestQueryPrice(bucket *utils.TokenBucket, tokenId string) (float64, error) {
	url := "https://clob.polymarket.com/midpoint?token_id=" + tokenId
	bucket.WaitForToken(time.Millisecond*250, "get market info")
	resp, err := http.Get(url)
	if err != nil {
		return 0, fmt.Errorf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body: %v", err)
	}

	var px utils.PolyMidPrice
	err = json.Unmarshal(body, &px)
	if err != nil {
		return 0, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}
	fmt.Printf("mid price = %.2f\n", px.Px)
	return float64(px.Px), nil
}

// RestQueryHistory queries historical prices on maximal time range plus 1 week for the minimal granularity of 1 minute
func RestQueryHistory(bucket *utils.TokenBucket, tokenId string) ([]utils.PolyHistory, error) {
	bucket.WaitForToken(time.Millisecond*250, "get market info")
	hMax, err := rawQueryHistory("https://clob.polymarket.com/prices-history?market=" + tokenId + "&interval=max")
	if err != nil {
		return nil, err
	}
	bucket.WaitForToken(time.Millisecond*250, "get market info")
	hWeek, err := rawQueryHistory("https://clob.polymarket.com/prices-history?market=" + tokenId + "&interval=1w&fidelity=1")
	if err != nil {
		return nil, err
	}
	obs := append(hMax, hWeek...)
	return obs, nil
}

// rawQueryHistory queries the given url for polyhistory data
func rawQueryHistory(url string) ([]utils.PolyHistory, error) {

	resp, err := http.Get(url)
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

	var ph utils.PolyHistoryResponse

	err = json.Unmarshal(body, &ph)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}
	return ph.History, nil
}
