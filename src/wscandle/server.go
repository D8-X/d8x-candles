package wscandle

import (
	"context"
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/gorilla/websocket"
	"github.com/redis/rueidis"
)

// Subscriptions is a type for each string of topic and the clients that subscribe to it
type Subscriptions map[string]Clients

// Clients is a type that describe the clients' ID and their connection
type Clients map[string]*ClientConn

type ClientConn struct {
	Conn *websocket.Conn
	Mu   sync.Mutex
}

// Server is the struct to handle the Server functions & manage the Subscriptions
type Server struct {
	Subscriptions     Subscriptions
	SubMu             sync.RWMutex                  // Mutex to protect Subscriptions
	LastCandles       map[string]*utils.OhlcData    //symbol:period->OHLC
	CandleMu          sync.RWMutex                  // Mutex to protect OhlcData updates
	MarketResponses   map[string]MarketResponse     //symbol->market response
	MarketMu          sync.RWMutex                  // Mutex to protect Market updates
	TickerToPriceType map[string]d8xUtils.PriceType //symbol->corresponding price type
	TickerMu          sync.RWMutex                  // Mutex to protect the ticker mapping
	RedisTSClient     *rueidis.Client
	MsgCount          int
	LastPxUpdtTs      int64
	LastPxUpdtTsMu    sync.RWMutex
}

func (srv *Server) SetLastPxUpdtTs() {
	srv.LastPxUpdtTsMu.Lock()
	defer srv.LastPxUpdtTsMu.Unlock()
	srv.LastPxUpdtTs = time.Now().Unix()
}
func (srv *Server) GetLastPxUpdtTs() int64 {
	srv.LastPxUpdtTsMu.Lock()
	defer srv.LastPxUpdtTsMu.Unlock()
	return srv.LastPxUpdtTs
}

type ClientMessage struct {
	Type  string `json:"type"`
	Topic string `json:"topic"`
}

type ServerResponse struct {
	Type  string      `json:"type"`
	Topic string      `json:"topic"`
	Data  interface{} `json:"data"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type MarketResponse struct {
	Sym           string  `json:"symbol"`
	AssetType     string  `json:"assetType"`
	Ret24hPerc    float64 `json:"ret24hPerc"`
	CurrentPx     float64 `json:"currentPx"`
	IsOpen        bool    `json:"isOpen"`
	NxtOpenTsSec  int64   `json:"nextOpen"`
	NxtCloseTsSec int64   `json:"nextClose"`
}

const MARKETS_TOPIC = "MARKETS"

func NewServer() *Server {
	var s = Server{
		Subscriptions:     make(Subscriptions),
		LastCandles:       make(map[string]*utils.OhlcData),
		MarketResponses:   make(map[string]MarketResponse),
		TickerToPriceType: make(map[string]d8xUtils.PriceType),
	}
	s.Subscriptions[MARKETS_TOPIC] = make(Clients)
	return &s
}

// Send simply sends message to the websocket client
func (srv *Server) Send(conn *ClientConn, message []byte) {
	// send simple message
	conn.Mu.Lock()
	defer conn.Mu.Unlock()
	conn.Conn.WriteMessage(websocket.TextMessage, message)
}

// SendWithWait sends message to the websocket client using wait group, allowing usage with goroutines
func (srv *Server) SendWithWait(conn *ClientConn, clientId string, message []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	conn.Mu.Lock()
	err := conn.Conn.WriteMessage(websocket.TextMessage, message)
	conn.Mu.Unlock()
	if err != nil {
		slog.Error("error sending message to client, removing client", "error", err.Error())
		srv.RemoveClient(clientId)
	}
}

// RemoveClient removes the clients from the server subscription map
func (srv *Server) RemoveClient(clientID string) {
	// loop all topics
	srv.SubMu.Lock()
	defer srv.SubMu.Unlock()
	num := 0
	for _, clients := range srv.Subscriptions {
		// delete the client from all the topic's client map
		delete(clients, clientID)
		num = max(num, len(clients))
	}
	slog.Info("removed client", "clientId", clientID, "max-num-subscr", num)
}

// Process incoming websocket message
// https://github.com/madeindra/golang-websocket/
func (srv *Server) HandleRequest(conn *ClientConn, cndlPeriods map[string]utils.CandlePeriod, clientID string, message []byte) {

	var data ClientMessage
	err := json.Unmarshal(message, &data)
	if err != nil {
		slog.Info(fmt.Sprintf("invalid request received: %s", string(message)))
		// JSON parsing not successful
		return
	}
	reqTopic := strings.TrimSpace(strings.ToUpper(data.Topic))
	reqType := strings.TrimSpace(strings.ToUpper(data.Type))
	slog.Info(fmt.Sprintf("request received %s %s", reqTopic, reqType))
	if reqType == "SUBSCRIBE" {
		if reqTopic == MARKETS_TOPIC {
			msg := srv.SubscribeMarkets(conn, clientID)
			srv.Send(conn, msg)
		} else {
			msg := srv.SubscribeCandles(conn, clientID, reqTopic, cndlPeriods)
			srv.Send(conn, msg)
		}
	} else if reqType == "UNSUBSCRIBE" {
		// unsubscribe
		srv.UnsubscribeTopic(clientID, reqTopic)

	} //else: ignore
}

// Unsubscribe the client from a candle-topic (e.g. btc-usd:15m)
func (srv *Server) UnsubscribeTopic(clientID string, topic string) {
	srv.SubMu.Lock()
	defer srv.SubMu.Unlock()
	// if topic exists, check the client map
	if _, exist := srv.Subscriptions[topic]; exist {
		clients := srv.Subscriptions[topic]
		// remove the client from the topic's client map
		delete(clients, clientID)
	}
}

// Subscribe the client to market updates (markets)
func (srv *Server) SubscribeMarkets(conn *ClientConn, clientID string) []byte {
	srv.subscribeTopic(conn, MARKETS_TOPIC, clientID)
	// data to send
	return srv.buildMarketResponse("subscribe")
}

// subscribeTopic subscribes a client to a topic. Creates topic if not existent.
// Returns true if the client is not subscribed yet
func (srv *Server) subscribeTopic(conn *ClientConn, topic string, clientID string) {
	srv.SubMu.Lock()
	defer srv.SubMu.Unlock()
	clients, exist := srv.Subscriptions[topic]
	if !exist {
		// if topic does not exist, create a new topic
		clients = make(Clients)
		srv.Subscriptions[topic] = clients
	}
	// if client not already subscribed, we add
	if _, subbed := clients[clientID]; !subbed {
		// not subscribed
		clients[clientID] = conn
	}
}

func (srv *Server) numSubscribers(topic string) int {
	srv.SubMu.RLock()
	defer srv.SubMu.RUnlock()
	return len(srv.Subscriptions[topic])
}

func (srv *Server) ScheduleUpdateMarketAndBroadcast(ctx context.Context, waitTime time.Duration) {
	tickerUpdate := time.NewTicker(waitTime)
	defer tickerUpdate.Stop()
	srv.UpdateMarketAndBroadcast()
	for {
		select {
		case <-ctx.Done():
			slog.Info("stopping market update scheduler")
			return
		case <-tickerUpdate.C: // if no subscribers we update infrequently
			if srv.numSubscribers(MARKETS_TOPIC) == 0 && rand.Float64() < 0.95 {
				slog.Info("UpdateMarketAndBroadcast: no subscribers")
				break
			}
			srv.UpdateMarketAndBroadcast()
			fmt.Println("Market info data updated.")
		}
	}

}

func (srv *Server) UpdateMarketAndBroadcast() {
	srv.UpdateMarketResponses()
	srv.SendMarketResponses()
}

func (srv *Server) buildMarketResponse(typeRes string) []byte {
	// create array of MarketResponses
	srv.MarketMu.RLock()
	var m = make([]MarketResponse, len(srv.MarketResponses))
	k := 0
	for _, el := range srv.MarketResponses {
		m[k] = el
		k += 1
	}
	srv.MarketMu.RUnlock()

	r := ServerResponse{Type: typeRes, Topic: strings.ToLower(MARKETS_TOPIC), Data: m}
	jsonData, err := json.Marshal(r)
	if err != nil {
		slog.Error("forming market update")
		return []byte{}
	}
	return jsonData
}

// copyClients copies the clients for a given topic,
// so that we don't need to hold the lock for too long
func (srv *Server) copyClients(topic string) Clients {
	srv.SubMu.RLock()
	defer srv.SubMu.RUnlock()
	originalClients, exists := srv.Subscriptions[topic]
	if !exists {
		return nil // Return nil if the topic doesn't exist
	}
	copiedClients := make(Clients, len(originalClients))
	for id, conn := range originalClients {
		copiedClients[id] = conn
	}
	return copiedClients
}

func (srv *Server) SendMarketResponses() {
	jsonData := srv.buildMarketResponse("update")
	clients := srv.copyClients(MARKETS_TOPIC)
	if clients == nil {
		// no subscribers
		return
	}
	var wg sync.WaitGroup
	for clientId, conn := range clients {
		wg.Add(1)
		go srv.SendWithWait(conn, clientId, jsonData, &wg)
	}
	wg.Wait()
}

// update market info for all symbols using Redis
// and store in srv.MarketResponses
func (srv *Server) UpdateMarketResponses() {

	nowUTCms := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	//yesterday:
	var anchorTime24hMs int64 = nowUTCms - 86400000
	// symbols
	c := *srv.RedisTSClient
	relevTypes := []d8xUtils.PriceType{
		d8xUtils.PXTYPE_PYTH,
		d8xUtils.PXTYPE_POLYMARKET,
		d8xUtils.PXTYPE_V2,
		d8xUtils.PXTYPE_V3,
	}
	for _, priceType := range relevTypes {
		key := utils.RDS_AVAIL_TICKER_SET + ":" + priceType.String()
		members, err := c.Do(context.Background(), c.B().Smembers().Key(key).Build()).AsStrSlice()
		if err != nil {
			slog.Error("UpdateMarketResponses:", "key", key, "error", err)
			continue
		}
		for _, sym := range members {
			srv.updtMarketForSym(sym, anchorTime24hMs)
		}
	}
}

func (srv *Server) getTickerToPriceType(sym string) (d8xUtils.PriceType, bool) {
	srv.TickerMu.RLock()
	defer srv.TickerMu.RUnlock()
	priceType, exists := srv.TickerToPriceType[sym]
	return priceType, exists
}

func (srv *Server) setTickerToPriceType(sym string, pxtype d8xUtils.PriceType) {
	srv.TickerMu.Lock()
	defer srv.TickerMu.Unlock()
	srv.TickerToPriceType[sym] = pxtype
}

func (srv *Server) updtMarketForSym(sym string, anchorTime24hMs int64) error {
	m, err := utils.RedisGetMarketInfo(context.Background(), srv.RedisTSClient, sym)
	if err != nil {
		return err
	}
	var pxtype d8xUtils.PriceType
	var exists bool
	pxtype, exists = srv.getTickerToPriceType(sym)
	if !exists {
		var avail, ready bool
		pxtype, avail, ready = srv.IsSymbolAvailable(sym)
		if !avail || !ready {
			return fmt.Errorf("sym %s not ready for market info", sym)
		}
		// store the "source" of the symbol
		srv.setTickerToPriceType(sym, pxtype)
	}
	px, err := utils.RedisTsGet(srv.RedisTSClient, sym, pxtype)
	if err != nil {
		return err
	}
	const d = 86400000
	px24, err := utils.RangeAggr(
		srv.RedisTSClient,
		sym,
		pxtype,
		anchorTime24hMs,
		anchorTime24hMs+d,
		60000,
	)
	var ret float64
	if err != nil || len(px24) == 0 || px24[0].O == 0 {
		px24 = nil
	} else {
		if m.AssetType == d8xUtils.ACLASS_POLYMKT {
			// absolute change for betting markets
			ret = px.Value - px24[0].O
		} else {
			ret = px.Value/px24[0].O - 1
			scale := float64(px.Timestamp-px24[0].TsMs) / float64(d)
			ret = ret * scale
		}
	}

	var mr = MarketResponse{
		Sym:           strings.ToLower(sym),
		AssetType:     m.AssetType.String(),
		Ret24hPerc:    ret * 100,
		CurrentPx:     px.Value,
		IsOpen:        m.MarketHours.IsOpen,
		NxtOpenTsSec:  m.MarketHours.NextOpen,
		NxtCloseTsSec: m.MarketHours.NextClose,
	}
	srv.MarketMu.Lock()
	srv.MarketResponses[sym] = mr
	srv.MarketMu.Unlock()
	return nil
}

// Subscribe the client to a candle-topic (e.g. btc-usd:15m)
func (srv *Server) SubscribeCandles(conn *ClientConn, clientID string, topic string, cndlPeriods map[string]utils.CandlePeriod) []byte {
	if !isValidCandleTopic(topic) {
		slog.Info("invalid candle topic requested:" + topic)
		return errorResponse("subscribe", topic, "usage: symbol:period")
	}
	sym, period, isFound := strings.Cut(topic, ":")
	if !isFound {
		// usage: symbol:period
		return errorResponse("subscribe", topic, "usage: symbol:period")
	}
	p := cndlPeriods[period]
	if p.TimeMs == 0 {
		// period not supported
		return errorResponse("subscribe", topic, "period not supported")
	}

	if _, exists := srv.getTickerToPriceType(sym); !exists {
		pxtype, avail, ready := srv.IsSymbolAvailable(sym)
		if pxtype == d8xUtils.PXTYPE_UNKNOWN {
			// symbol not supported
			return errorResponse("subscribe", topic, "symbol not supported")
		}
		// store the "source" of the symbol
		srv.setTickerToPriceType(sym, pxtype)
		if avail && !ready {
			// symbol not available
			redisSendTickerRequest(srv.RedisTSClient, sym)
			return errorResponse("subscribe", topic, "symbol not available yet")
		}
	}

	pxtype, _ := srv.getTickerToPriceType(sym)
	if pxtype == d8xUtils.PXTYPE_PYTH || pxtype == d8xUtils.PXTYPE_POLYMARKET {
		redisSendTickerRequest(srv.RedisTSClient, sym)
	}
	srv.subscribeTopic(conn, topic, clientID)
	return srv.candleResponse(sym, p)
}

func redisSendTickerRequest(client *rueidis.Client, sym string) {
	// we send the ticker request even if available (other process could have crashed)
	// if the symbol can be triangulated, this will be done now
	slog.Info("sending ticker request", "symbol", sym)
	c := *client
	err := c.Do(
		context.Background(),
		c.B().Publish().Channel(utils.RDS_TICKER_REQUEST).Message(sym).Build(),
	).Error()
	if err != nil {
		slog.Error("IsSymbolAvailable " + sym + "error:" + err.Error())
	}
}

// IsSymbolAvailable returns the price source, whether the symbol can be
// triangulated and whether it is readily available
func (srv *Server) IsSymbolAvailable(sym string) (d8xUtils.PriceType, bool, bool) {
	// first priority: Pyth-type
	if utils.RedisIsSymbolAvailable(srv.RedisTSClient, d8xUtils.PXTYPE_PYTH, sym) {
		return d8xUtils.PXTYPE_PYTH, true, true
	}
	if utils.RedisIsSymbolAvailable(srv.RedisTSClient, d8xUtils.PXTYPE_POLYMARKET, sym) {
		return d8xUtils.PXTYPE_POLYMARKET, true, true
	}
	ccys := strings.Split(sym, "-")
	avail, err := utils.RedisAreCcyAvailable(srv.RedisTSClient, d8xUtils.PXTYPE_PYTH, ccys)
	if err == nil && avail[0] && avail[1] {
		return d8xUtils.PXTYPE_PYTH, true, false
	}
	avail, err = utils.RedisAreCcyAvailable(srv.RedisTSClient, d8xUtils.PXTYPE_POLYMARKET, ccys)
	if err == nil && avail[0] && avail[1] {
		return d8xUtils.PXTYPE_POLYMARKET, true, false
	}
	if utils.RedisIsSymbolAvailable(srv.RedisTSClient, d8xUtils.PXTYPE_POLYMARKET, sym) {
		return d8xUtils.PXTYPE_POLYMARKET, true, true
	}
	// V2 and V3 are not triangulated on demand, hence we directly query the symbol
	if utils.RedisIsSymbolAvailable(srv.RedisTSClient, d8xUtils.PXTYPE_V2, sym) {
		return d8xUtils.PXTYPE_V2, true, true
	}
	if utils.RedisIsSymbolAvailable(srv.RedisTSClient, d8xUtils.PXTYPE_V3, sym) {
		return d8xUtils.PXTYPE_V3, true, true
	}
	return d8xUtils.PXTYPE_UNKNOWN, false, false
}

func isValidCandleTopic(topic string) bool {
	pattern := "^[a-zA-Z0-9.]+-[a-zA-Z0-9]+:[0-9]+[HMD]$" // Regular expression for candle topics
	regex, _ := regexp.Compile(pattern)
	return regex.MatchString(topic)
}

// form initial response for candle subscription (e.g., eth-usd:5m)
func (srv *Server) candleResponse(sym string, p utils.CandlePeriod) []byte {
	pxtype, _ := srv.getTickerToPriceType(sym)
	slog.Info("Subscription for symbol " +
		pxtype.String() + ":" + sym +
		" Period " + fmt.Sprint(p.TimeMs/60000) + "m")
	data := GetInitialCandles(srv.RedisTSClient, sym, pxtype, p)
	topic := sym + ":" + p.Name
	if data == nil {
		// return empty array [] instead of null
		data = []utils.OhlcData{}
	}
	res := ServerResponse{Type: "subscribe", Topic: strings.ToLower(topic), Data: data}
	jsonData, err := json.Marshal(res)
	if err != nil {
		slog.Error("candle res")
	}
	return jsonData
}

func errorResponse(reqType string, reqTopic string, msg string) []byte {

	e := ErrorResponse{Error: msg}
	res := ServerResponse{Type: reqType, Topic: strings.ToLower(reqTopic), Data: e}
	jsonData, err := json.Marshal(res)
	if err != nil {
		slog.Error("forming error response")
	}
	return jsonData
}

func (srv *Server) IsPxSubscriptionExpired() bool {
	return time.Now().Unix()-srv.GetLastPxUpdtTs() > 5*60
}

// subscribe the server to rueidis pub/sub
func (srv *Server) HandlePxUpdateFromRedis(msg rueidis.PubSubMessage, candlePeriodsMs map[string]utils.CandlePeriod) {
	srv.MsgCount++
	if srv.MsgCount%100 == 0 {
		srv.SetLastPxUpdtTs()
	}
	if srv.MsgCount%500 == 0 {
		slog.Info(fmt.Sprintf("REDIS received %d messages since last report (now: %s)", srv.MsgCount, msg.Message))
		srv.MsgCount = 0
	}
	symbols := strings.Split(msg.Message, ";")
	for j := range symbols {
		symbols[j] = strings.Split(symbols[j], ":")[1]
	}
	srv.candleUpdates(symbols, candlePeriodsMs)
}

// process price updates triggered by redis pub message for candles
func (srv *Server) candleUpdates(symbols []string, candlePeriodsMs map[string]utils.CandlePeriod) {

	var wg sync.WaitGroup
	for _, sym := range symbols {
		pxtype, exists := srv.getTickerToPriceType(sym)
		if !exists {
			continue
		}
		pxLast, err := utils.RedisTsGet(srv.RedisTSClient, sym, pxtype)

		if err != nil {
			slog.Error(fmt.Sprintf("Error parsing date:" + err.Error()))
			return
		}
		for _, period := range candlePeriodsMs {
			key := sym + ":" + period.Name
			// update subscribers
			clients := srv.copyClients(key)
			if clients == nil {
				// no need to update the candle if we have no subscribers
				// but we need to ensure the candle is remove when
				// we had no more subscribers and start again
				continue
			}
			// updating the given candle with the new price (concurrency-safe) and
			// using the copy to continue
			candle := srv.updateOhlc(sym, &period, &pxLast)
			r := ServerResponse{Type: "update", Topic: strings.ToLower(key), Data: candle}
			jsonData, err := json.Marshal(r)
			if err != nil {
				slog.Error("forming lastCandle update:" + err.Error())
			}
			for clientId, conn := range clients {
				wg.Add(1)
				go srv.SendWithWait(conn, clientId, jsonData, &wg)
			}
		}
	}
	// wait until all goroutines jobs done
	wg.Wait()
}

// updateOhlc updates the candle for the given key(=symbol:period) with the given
// datapoint. Also recognizes if candle is outdated
func (srv *Server) updateOhlc(sym string, period *utils.CandlePeriod, pxLast *utils.DataPoint) utils.OhlcData {
	srv.CandleMu.Lock()
	defer srv.CandleMu.Unlock()
	key := sym + ":" + period.Name
	lastCandle := srv.LastCandles[key]
	if lastCandle == nil {
		srv.LastCandles[key] = &utils.OhlcData{}
		lastCandle = srv.LastCandles[key]
	}
	if pxLast.Timestamp > lastCandle.TsMs+int64(period.TimeMs) {
		// new candle
		lastCandle.O = pxLast.Value
		lastCandle.H = pxLast.Value
		lastCandle.L = pxLast.Value
		lastCandle.C = pxLast.Value
		nextTs := (pxLast.Timestamp / int64(period.TimeMs)) * int64(period.TimeMs)
		lastCandle.TsMs = nextTs
		lastCandle.Time = utils.ConvertTimestampToISO8601(nextTs)
	} else {
		// update existing candle
		lastCandle.C = pxLast.Value
		lastCandle.H = math.Max(pxLast.Value, lastCandle.H)
		lastCandle.L = math.Min(pxLast.Value, lastCandle.L)
	}
	// copy
	candle := *lastCandle
	return candle
}
