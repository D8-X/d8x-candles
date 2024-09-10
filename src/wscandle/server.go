package wscandle

import (
	"context"
	"d8x-candles/src/pythclient"
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

	"github.com/gorilla/websocket"
	redis "github.com/redis/go-redis/v9"
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
	Subscriptions   Subscriptions
	LastCandles     map[string]*pythclient.OhlcData //symbol:period->OHLC
	MarketResponses map[string]MarketResponse       //symbol->market response
	RedisTSClient   *utils.RueidisClient
	MsgCount        int
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
	var s Server
	s.Subscriptions = make(Subscriptions)
	s.Subscriptions[MARKETS_TOPIC] = make(Clients)
	s.LastCandles = make(map[string]*pythclient.OhlcData)
	s.MarketResponses = make(map[string]MarketResponse)
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
	for _, client := range s.Subscriptions {
		// delete the client from all the topic's client map
		delete(client, clientID)
	}
}

// Process incoming websocket message
// https://github.com/madeindra/golang-websocket/
func (s *Server) HandleRequest(conn *ClientConn, config utils.SymbolManager, clientID string, message []byte) {

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
			server.Send(conn, msg)
		} else {
			msg := s.SubscribeCandles(conn, clientID, reqTopic, config)
			server.Send(conn, msg)
		}
	} else if reqType == "UNSUBSCRIBE" {
		// unsubscribe
		if reqTopic == MARKETS_TOPIC {
			delete(s.Subscriptions[reqTopic], clientID)
		} else {
			server.UnsubscribeCandles(clientID, reqTopic)
		}
	} //else: ignore
}

// Unsubscribe the client from a candle-topic (e.g. btc-usd:15m)
func (s *Server) UnsubscribeCandles(clientID string, topic string) {
	// if topic exists, check the client map
	if _, exist := s.Subscriptions[topic]; exist {
		client := s.Subscriptions[topic]
		// remove the client from the topic's client map
		delete(client, clientID)
	}
}

// Subscribe the client to market updates (markets)
func (s *Server) SubscribeMarkets(conn *ClientConn, clientID string) []byte {
	clients := s.Subscriptions[MARKETS_TOPIC]
	// if client already subscribed, stop the process
	if _, subbed := clients[clientID]; subbed {
		return errorResponse("subscribe", MARKETS_TOPIC, "client already subscribed")
	}
	// if not subbed, add to client map
	clients[clientID] = conn
	// data to send
	return s.buildMarketResponse("subscribe")
}

func (s *Server) ScheduleUpdateMarketAndBroadcast(waitTime time.Duration, config utils.SymbolManager) {
	tickerUpdate := time.NewTicker(waitTime)
	s.UpdateMarketAndBroadcast()
	for {
		select {
		case <-tickerUpdate.C:
			// if no subscribers we update infrequently
			if len(server.Subscriptions[MARKETS_TOPIC]) == 0 &&
				rand.Float64() < 0.95 {
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

func (s *Server) SendMarketResponses() {
	jsonData := s.buildMarketResponse("update")
	// update subscribers
	clients := server.Subscriptions[MARKETS_TOPIC]
	var wg sync.WaitGroup
	for _, conn := range clients {
		wg.Add(1)
		go server.SendWithWait(conn, jsonData, &wg)
	}
	wg.Wait()
}

// update market info for all symbols using Redis
func (s *Server) UpdateMarketResponses() {

	nowUTCms := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	//yesterday:
	var anchorTime24hMs int64 = nowUTCms - 86400000
	// symbols
	c := *s.RedisTSClient.Client
	members, err := c.Do(context.Background(), c.B().Smembers().Key(utils.AVAIL_TICKER_SET).Build()).AsStrSlice()
	if err != nil {
		slog.Error("UpdateMarketResponses:" + err.Error())
		return
	}
	for _, sym := range members {
		s.updtMarketForSym(sym, anchorTime24hMs)
	}
}

func (s *Server) updtMarketForSym(sym string, anchorTime24hMs int64) error {
	m, err := utils.GetMarketInfo(s.RedisTSClient.Ctx, s.RedisTSClient.Client, sym)
	if err != nil {
		return err
	}
	px, err := s.RedisTSClient.Get(sym)
	if err != nil {
		return err
	}
	const d = 86400000
	px24, err := s.RedisTSClient.RangeAggr(sym, anchorTime24hMs, anchorTime24hMs+d, 60000, "first")
	var ret float64
	if err != nil || len(px24) == 0 || px24[0].Value == 0 {
		px24 = nil
	} else {
		if m.AssetType == utils.POLYMARKET_TYPE {
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
		AssetType:     m.AssetType,
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
func (s *Server) SubscribeCandles(conn *ClientConn, clientID string, topic string, config utils.SymbolManager) []byte {
	if !isValidCandleTopic(topic) {
		slog.Info("invalid candle topic requested:" + topic)
		return errorResponse("subscribe", topic, "usage: symbol:period")
	}
	sym, period, isFound := strings.Cut(topic, ":")
	if !isFound {
		// usage: symbol:period
		return errorResponse("subscribe", topic, "usage: symbol:period")
	}
	if !s.IsSymbolAvailable(sym) {
		// symbol not supported
		return errorResponse("subscribe", topic, "symbol not supported")
	}
	p := config.CandlePeriodsMs[period]
	if p.TimeMs == 0 {
		// period not supported
		return errorResponse("subscribe", topic, "period not supported")
	}

	if _, exist := s.Subscriptions[topic]; exist {
		clients := s.Subscriptions[topic]
		// if client already subscribed, stop the process
		if _, subbed := clients[clientID]; subbed {
			return errorResponse("subscribe", topic, "client already subscribed")
		}
		// not subscribed
		clients[clientID] = conn
		return s.candleResponse(sym, p)
	}

	// if topic does not exist, create a new topic
	newClient := make(Clients)
	s.Subscriptions[topic] = newClient

	// add the client to the topic
	s.Subscriptions[topic][clientID] = conn
	return s.candleResponse(sym, p)
}

func (s *Server) IsSymbolAvailable(sym string) bool {
	// check redis
	c := *s.RedisTSClient.Client
	isMember, err := c.Do(context.Background(), c.B().Sismember().Key(utils.AVAIL_TICKER_SET).Member(sym).Build()).AsBool()
	if err != nil {
		slog.Error("IsSymbolAvailable " + sym + "error:" + err.Error())
		return false
	}
	// we send the ticker request even if available (other process could have crashed)
	// if the symbol can be triangulated, this will be done now
	err = c.Do(context.Background(), c.B().Publish().Channel(utils.TICKER_REQUEST).Message(sym).Build()).Error()
	if err != nil {
		slog.Error("IsSymbolAvailable " + sym + "error:" + err.Error())
	}
	return isMember
}

func isValidCandleTopic(topic string) bool {
	pattern := "^[a-zA-Z0-9]+-[a-zA-Z0-9]+:[0-9]+[HMD]$" // Regular expression for candle topics
	regex, _ := regexp.Compile(pattern)
	return regex.MatchString(topic)
}

// form initial response for candle subscription (e.g., eth-usd:5m)
func (s *Server) candleResponse(sym string, p utils.CandlePeriod) []byte {
	slog.Info("Subscription for symbol " + sym + " Period " + fmt.Sprint(p.TimeMs/60000) + "m")
	data := GetInitialCandles(s.RedisTSClient, sym, p)
	topic := sym + ":" + p.Name
	if data == nil {
		// return empty array [] instead of null
		data = []pythclient.OhlcData{}
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

// subscribe the server to redis pub/sub
func (s *Server) SubscribePxUpdate(sub *redis.PubSub, ctx context.Context) {
	for {
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			panic(err)
		}
		s.MsgCount++
		if s.MsgCount%500 == 0 {
			slog.Info(fmt.Sprintf("REDIS received %d messages since last report (now: %s)", s.MsgCount, msg.Payload))
			s.MsgCount = 0
		}

		symbols := strings.Split(msg.Payload, ";")
		s.candleUpdates(symbols)
	}
}

// process price updates triggered by redis pub message for
// candles
func (s *Server) candleUpdates(symbols []string) {

	var wg sync.WaitGroup
	for _, sym := range symbols {
		c := (*s.RedisTSClient)
		pxLast, err := c.Get(sym)

		if err != nil {
			slog.Error(fmt.Sprintf("Error parsing date:" + err.Error()))
			return
		}
		for _, prd := range config.CandlePeriodsMs {
			key := sym + ":" + prd.Name
			lastCandle := s.LastCandles[key]
			if lastCandle == nil {
				s.LastCandles[key] = &pythclient.OhlcData{}
				lastCandle = s.LastCandles[key]
			}
			if pxLast.Timestamp > lastCandle.TsMs+int64(prd.TimeMs) {
				// new candle
				lastCandle.O = pxLast.Value
				lastCandle.H = pxLast.Value
				lastCandle.L = pxLast.Value
				lastCandle.C = pxLast.Value
				nextTs := (pxLast.Timestamp / int64(prd.TimeMs)) * int64(prd.TimeMs)
				lastCandle.TsMs = nextTs
				lastCandle.Time = pythclient.ConvertTimestampToISO8601(nextTs)
			} else {
				// update existing candle
				lastCandle.C = pxLast.Value
				lastCandle.H = math.Max(pxLast.Value, lastCandle.H)
				lastCandle.L = math.Min(pxLast.Value, lastCandle.L)
			}
			// update subscribers
			clients := server.Subscriptions[key]
			r := ServerResponse{Type: "update", Topic: strings.ToLower(key), Data: lastCandle}
			jsonData, err := json.Marshal(r)
			if err != nil {
				slog.Error("forming lastCandle update:" + err.Error())
			}
			for _, conn := range clients {
				wg.Add(1)
				go server.SendWithWait(conn, jsonData, &wg)
			}
		}
	}
	// wait until all goroutines jobs done
	wg.Wait()
}
