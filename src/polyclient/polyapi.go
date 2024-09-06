package polyclient

import (
	"d8x-candles/src/utils"
	"encoding/json"
	"errors"
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
	AssetIds     map[string]string //dec id -> symbol
	LastUpdate   map[string]int64  //dec id -> timestamp
	MuLastUpdate sync.Mutex
	MuAssets     sync.Mutex
	MuWs         sync.Mutex
	Ws           *websocket.Conn
	reconnChan   chan struct{}
	apiBucket    *utils.TokenBucket
	oracleEndpt  string
}

func NewPolyApi(oracleEndpt string) *PolyApi {

	pa := PolyApi{
		AssetIds:     make(map[string]string),
		LastUpdate:   make(map[string]int64),
		MuLastUpdate: sync.Mutex{},
		MuAssets:     sync.Mutex{},
		MuWs:         sync.Mutex{},
		Ws:           nil,
		reconnChan:   make(chan struct{}),
		apiBucket:    utils.NewTokenBucket(4, 4.0),
		oracleEndpt:  oracleEndpt,
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

func GetMarketInfo(bucket *utils.TokenBucket, conditionIdHex string) (*utils.PolyMarketInfo, error) {
	endpt := "https://clob.polymarket.com/markets/" + conditionIdHex
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

	type PolyMarketInfoJSON struct {
		Active          bool              `json:"active"`
		Closed          bool              `json:"closed"`
		AcceptingOrders bool              `json:"accepting_orders"`
		EndDateISO      string            `json:"end_date_iso"`
		Tokens          []utils.PolyToken `json:"tokens"`
	}
	var marketInfo PolyMarketInfoJSON
	err = json.Unmarshal(body, &marketInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}
	var endDateTs int64
	endDate, err := time.Parse(time.RFC3339, marketInfo.EndDateISO)
	if err != nil {
		slog.Error(fmt.Sprintf("GetMarketInfo: parsing time %s: %v", marketInfo.EndDateISO, err.Error()))
	}
	endDateTs = endDate.Unix()
	marketInfoParsed := utils.PolyMarketInfo{
		Active:          marketInfo.Active,
		Closed:          marketInfo.Closed,
		AcceptingOrders: marketInfo.AcceptingOrders,
		EndDateISOTs:    endDateTs,
		Tokens:          marketInfo.Tokens,
	}
	return &marketInfoParsed, nil
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
				fmt.Printf("error handling event: %v\n", err)
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
	fmt.Printf("Polymarkets sending subscribe message: %s\n", message)
	err = pa.Ws.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		return fmt.Errorf("failed to write subscribe message: %w", err)
	}
	return nil
}

// handleEvent parses ws-subscriptions-clob.polymarket events
func (pa *PolyApi) handleEvent(eventJson string, pc *PolyClient) error {
	if eventJson == "Invalid command" {
		return errors.New(eventJson)
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

	pa.MuLastUpdate.Lock()
	if int64(priceChange.TimestampMs) < pa.LastUpdate[id] {
		pa.MuLastUpdate.Unlock()
		slog.Info("skipping delayed message")
		return nil
	}
	pa.LastUpdate[id] = int64(priceChange.TimestampMs)
	pa.MuLastUpdate.Unlock()

	fmt.Printf("asset=%s price = %s\n", pa.AssetIds[priceChange.AssetID], priceChange.Price)
	px0, err := pa.restQueryPrice(priceChange.AssetID, pc)
	if err != nil {
		return fmt.Errorf("error querying price: %v", err)
	}
	if pc != nil {
		// update redis
		pc.OnNewPrice(pa.AssetIds[priceChange.AssetID], px0, int64(priceChange.TimestampMs))
	}
	return nil
}

func (pa *PolyApi) restQueryPrice(tokenIdDec string, pc *PolyClient) (float64, error) {
	sym := pa.AssetIds[tokenIdDec]
	id := pc.priceFeedUniverse[sym]
	if id.StorkSym != "" {
		// query price from stork API
		slog.Info("query stork for " + sym + "/" + id.StorkSym)
		return pc.stork.RestFetchStorkPrice(id.StorkSym)
	}
	slog.Info("query polymarket api for " + sym + "/" + tokenIdDec)
	// query price from polymarket api
	return RestQueryPrice(pa.apiBucket, tokenIdDec)
}

// RestQueryOracle queries index price and EMA price (mark price=ema+spread) from
// the oracle. Prices are transformed to 'probability'.
func RestQueryOracle(endPtUrl, tokenId string) (float64, float64, int64, error) {
	tokenIdHex, err := utils.Dec2Hex(tokenId)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid token id: %v", err)
	}
	url := fmt.Sprintf("%s/v2/updates/price/latest?ids[]=%s", endPtUrl, tokenIdHex)
	resp, err := http.Get(url)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, 0, 0, fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to read response body: %v", err)
	}
	var p utils.PythStreamData
	err = json.Unmarshal(body, &p)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to read response body: %v", err)
	}
	ema := p.Parsed[0].EMAPrice.CalcPrice()
	idx := p.Parsed[0].Price.CalcPrice()
	return idx - 1, ema - 1, p.Parsed[0].Price.PublishTime, nil
}

// RestQueryPrice queries the mid-price for the given token id (decimal) from
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
	hWeek, err := rawQueryHistory("https://clob.polymarket.com/prices-history?market=" + tokenId + "&interval=1w&fidelity=5")
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
