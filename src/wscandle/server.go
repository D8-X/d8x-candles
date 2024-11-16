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
	MarketResponses   map[string]MarketResponse     //symbol->market response
	TickerToPriceType map[string]d8xUtils.PriceType //symbol->corresponding price type
	RedisTSClient     *rueidis.Client
	MsgCount          int
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
func (s *Server) Send(conn *ClientConn, message []byte) {
	// send simple message
	conn.Mu.Lock()
	defer conn.Mu.Unlock()
	conn.Conn.WriteMessage(websocket.TextMessage, message)
}

// SendWithWait sends message to the websocket client using wait group, allowing usage with goroutines
func (s *Server) SendWithWait(conn *ClientConn, message []byte, wg *sync.WaitGroup) {
	// send simple message
	conn.Mu.Lock()
	defer conn.Mu.Unlock()
	conn.Conn.WriteMessage(websocket.TextMessage, message)

	// set the task as done
	wg.Done()
}

// RemoveClient removes the clients from the server subscription map
func (s *Server) RemoveClient(clientID string) {
	// loop all topics
	s.SubMu.Lock()
	defer s.SubMu.Unlock()
	for _, client := range s.Subscriptions {
		// delete the client from all the topic's client map
		delete(client, clientID)
	}
}

// Process incoming websocket message
// https://github.com/madeindra/golang-websocket/
func (s *Server) HandleRequest(conn *ClientConn, cndlPeriods map[string]utils.CandlePeriod, clientID string, message []byte) {

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
			msg := s.SubscribeMarkets(conn, clientID)
			s.Send(conn, msg)
		} else {
			msg := s.SubscribeCandles(conn, clientID, reqTopic, cndlPeriods)
			s.Send(conn, msg)
		}
	} else if reqType == "UNSUBSCRIBE" {
		// unsubscribe
		s.UnsubscribeTopic(clientID, reqTopic)

	} //else: ignore
}

// Unsubscribe the client from a candle-topic (e.g. btc-usd:15m)
func (s *Server) UnsubscribeTopic(clientID string, topic string) {
	s.SubMu.Lock()
	defer s.SubMu.Unlock()
	// if topic exists, check the client map
	if _, exist := s.Subscriptions[topic]; exist {
		clients := s.Subscriptions[topic]
		// remove the client from the topic's client map
		delete(clients, clientID)
	}
}

// Subscribe the client to market updates (markets)
func (s *Server) SubscribeMarkets(conn *ClientConn, clientID string) []byte {
	s.subscribeTopic(conn, MARKETS_TOPIC, clientID)
	// data to send
	return s.buildMarketResponse("subscribe")
}

// subscribeTopic subscribes a client to a topic. Creates topic if not existent.
// Returns true if the client is not subscribed yet
func (s *Server) subscribeTopic(conn *ClientConn, topic string, clientID string) {
	s.SubMu.Lock()
	defer s.SubMu.Unlock()
	clients, exist := s.Subscriptions[topic]
	if !exist {
		// if topic does not exist, create a new topic
		clients = make(Clients)
		s.Subscriptions[topic] = clients
	}
	// if client not already subscribed, we add
	if _, subbed := clients[clientID]; !subbed {
		// not subscribed
		clients[clientID] = conn
	}
}

func (s *Server) numSubscribers(topic string) int {
	s.SubMu.RLock()
	defer s.SubMu.RUnlock()
	return len(s.Subscriptions[topic])
}

func (s *Server) ScheduleUpdateMarketAndBroadcast(ctx context.Context, waitTime time.Duration) {
	tickerUpdate := time.NewTicker(waitTime)
	defer tickerUpdate.Stop()
	s.UpdateMarketAndBroadcast()
	for {
		select {
		case <-ctx.Done():
			slog.Info("stopping market update scheduler")
			return
		case <-tickerUpdate.C: // if no subscribers we update infrequently
			if s.numSubscribers(MARKETS_TOPIC) == 0 && rand.Float64() < 0.95 {
				slog.Info("UpdateMarketAndBroadcast: no subscribers")
				break
			}
			s.UpdateMarketAndBroadcast()
			fmt.Println("Market info data updated.")
		}
	}

}

func (s *Server) UpdateMarketAndBroadcast() {
	s.UpdateMarketResponses()
	s.SendMarketResponses()
}

func (s *Server) buildMarketResponse(typeRes string) []byte {
	// create array of MarketResponses
	var m = make([]MarketResponse, len(s.MarketResponses))
	k := 0
	for _, el := range s.MarketResponses {
		m[k] = el
		k += 1
	}
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
func (s *Server) copyClients(topic string) Clients {
	s.SubMu.RLock()
	defer s.SubMu.RUnlock()
	originalClients, exists := s.Subscriptions[topic]
	if !exists {
		return nil // Return nil if the topic doesn't exist
	}
	copiedClients := make(Clients, len(originalClients))
	for id, conn := range originalClients {
		copiedClients[id] = conn
	}
	return copiedClients
}

func (s *Server) SendMarketResponses() {
	jsonData := s.buildMarketResponse("update")
	clients := s.copyClients(MARKETS_TOPIC)
	if clients == nil {
		// no subscribers
		return
	}
	var wg sync.WaitGroup
	for _, conn := range clients {
		wg.Add(1)
		go s.SendWithWait(conn, jsonData, &wg)
	}
	wg.Wait()
}

// update market info for all symbols using Redis
func (s *Server) UpdateMarketResponses() {

	nowUTCms := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	//yesterday:
	var anchorTime24hMs int64 = nowUTCms - 86400000
	// symbols
	c := *s.RedisTSClient
	relevTypes := []d8xUtils.PriceType{
		d8xUtils.PXTYPE_PYTH,
		d8xUtils.PXTYPE_POLYMARKET,
		d8xUtils.PXTYPE_V2,
		d8xUtils.PXTYPE_V3,
	}
	for _, priceType := range relevTypes {
		key := utils.RDS_AVAIL_TICKER_SET + priceType.String()
		members, err := c.Do(context.Background(), c.B().Smembers().Key(key).Build()).AsStrSlice()
		if err != nil {
			slog.Error("UpdateMarketResponses:", "key", key, "error", err)
			continue
		}
		for _, sym := range members {
			s.updtMarketForSym(sym, anchorTime24hMs)
		}
	}
}

func (s *Server) updtMarketForSym(sym string, anchorTime24hMs int64) error {
	m, err := utils.RedisGetMarketInfo(context.Background(), s.RedisTSClient, sym)
	if err != nil {
		return err
	}
	px, err := utils.RedisTsGet(s.RedisTSClient, sym, s.TickerToPriceType[sym])
	if err != nil {
		return err
	}
	const d = 86400000
	px24, err := utils.RangeAggr(
		s.RedisTSClient,
		sym,
		s.TickerToPriceType[sym],
		anchorTime24hMs,
		anchorTime24hMs+d,
		60000,
		utils.AGGR_FIRST,
	)
	var ret float64
	if err != nil || len(px24) == 0 || px24[0].Value == 0 {
		px24 = nil
	} else {
		if m.AssetType == d8xUtils.ACLASS_POLYMKT {
			// absolute change for betting markets
			ret = px.Value - px24[0].Value
		} else {
			ret = px.Value/px24[0].Value - 1
			scale := float64(px.Timestamp-px24[0].Timestamp) / float64(d)
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
	s.MarketResponses[sym] = mr
	return nil
}

// Subscribe the client to a candle-topic (e.g. btc-usd:15m)
func (s *Server) SubscribeCandles(conn *ClientConn, clientID string, topic string, cndlPeriods map[string]utils.CandlePeriod) []byte {
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

	if _, exists := s.TickerToPriceType[sym]; !exists {
		pxtype, avail, ready := s.IsSymbolAvailable(sym)
		if pxtype == d8xUtils.PXTYPE_UNKNOWN {
			// symbol not supported
			return errorResponse("subscribe", topic, "symbol not supported")
		}
		if avail && !ready {
			// symbol not available
			redisSendTickerRequest(s.RedisTSClient, sym)
			return errorResponse("subscribe", topic, "symbol not available yet")
		}
		// store the "source" of the symbol
		s.TickerToPriceType[sym] = pxtype
	}
	if s.TickerToPriceType[sym] == d8xUtils.PXTYPE_PYTH {
		redisSendTickerRequest(s.RedisTSClient, sym)
	}
	s.subscribeTopic(conn, topic, clientID)
	return s.candleResponse(sym, p)
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
func (s *Server) IsSymbolAvailable(sym string) (d8xUtils.PriceType, bool, bool) {
	// first priority: Pyth-type
	if utils.RedisIsSymbolAvailable(s.RedisTSClient, d8xUtils.PXTYPE_PYTH, sym) {
		return d8xUtils.PXTYPE_PYTH, true, true
	}
	ccys := strings.Split(sym, "-")
	avail, err := utils.RedisAreCcyAvailable(s.RedisTSClient, d8xUtils.PXTYPE_PYTH, ccys)
	if err == nil && avail[0] && avail[1] {
		return d8xUtils.PXTYPE_PYTH, false, true
	}
	// V2 and V3 are not triangulated on demand, hence we directly query the symbol
	if utils.RedisIsSymbolAvailable(s.RedisTSClient, d8xUtils.PXTYPE_V2, sym) {
		return d8xUtils.PXTYPE_V2, true, true
	}
	if utils.RedisIsSymbolAvailable(s.RedisTSClient, d8xUtils.PXTYPE_V3, sym) {
		return d8xUtils.PXTYPE_V3, true, true
	}
	return d8xUtils.PXTYPE_UNKNOWN, false, false
}

func isValidCandleTopic(topic string) bool {
	pattern := "^[a-zA-Z0-9]+-[a-zA-Z0-9]+:[0-9]+[HMD]$" // Regular expression for candle topics
	regex, _ := regexp.Compile(pattern)
	return regex.MatchString(topic)
}

// form initial response for candle subscription (e.g., eth-usd:5m)
func (s *Server) candleResponse(sym string, p utils.CandlePeriod) []byte {
	pxtype := s.TickerToPriceType[sym]
	slog.Info("Subscription for symbol " +
		pxtype.String() + ":" + sym +
		" Period " + fmt.Sprint(p.TimeMs/60000) + "m")
	data := GetInitialCandles(s.RedisTSClient, sym, pxtype, p)
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

// subscribe the server to rueidis pub/sub
func (s *Server) HandlePxUpdateFromRedis(msg rueidis.PubSubMessage, candlePeriodsMs map[string]utils.CandlePeriod) {
	s.MsgCount++
	if s.MsgCount%500 == 0 {
		slog.Info(fmt.Sprintf("REDIS received %d messages since last report (now: %s)", s.MsgCount, msg.Message))
		s.MsgCount = 0
	}
	symbols := strings.Split(msg.Message, ";")
	for j := range symbols {
		symbols[j] = strings.Split(symbols[j], ":")[1]
	}
	s.candleUpdates(symbols, candlePeriodsMs)
}

// process price updates triggered by redis pub message for candles
func (s *Server) candleUpdates(symbols []string, candlePeriodsMs map[string]utils.CandlePeriod) {

	var wg sync.WaitGroup
	for _, sym := range symbols {
		pxtype, exists := s.TickerToPriceType[sym]
		if !exists {
			continue
		}
		pxLast, err := utils.RedisTsGet(s.RedisTSClient, sym, pxtype)

		if err != nil {
			slog.Error(fmt.Sprintf("Error parsing date:" + err.Error()))
			return
		}
		for _, period := range candlePeriodsMs {
			key := sym + ":" + period.Name
			lastCandle := s.LastCandles[key]
			if lastCandle == nil {
				s.LastCandles[key] = &utils.OhlcData{}
				lastCandle = s.LastCandles[key]
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
			// update subscribers
			clients := s.copyClients(key)
			r := ServerResponse{Type: "update", Topic: strings.ToLower(key), Data: lastCandle}
			jsonData, err := json.Marshal(r)
			if err != nil {
				slog.Error("forming lastCandle update:" + err.Error())
			}
			for _, conn := range clients {
				wg.Add(1)
				go s.SendWithWait(conn, jsonData, &wg)
			}
		}
	}
	// wait until all goroutines jobs done
	wg.Wait()
}
