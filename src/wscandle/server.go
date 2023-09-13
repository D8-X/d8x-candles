package wscandle

import (
	"context"
	"d8x-candles/src/builder"
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
type Clients map[string]*websocket.Conn

// Server is the struct to handle the Server functions & manage the Subscriptions
type Server struct {
	Subscriptions   Subscriptions
	LastCandles     map[string]*builder.OhlcData //symbol:period->OHLC
	MarketResponses map[string]MarketResponse    //symbol->market response
	RedisTSClient   *utils.RueidisClient
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

const MARKETS_TOPIC = "markets"

func NewServer() *Server {
	var s Server
	s.Subscriptions = make(Subscriptions)
	s.Subscriptions[MARKETS_TOPIC] = make(Clients)
	s.LastCandles = make(map[string]*builder.OhlcData)
	s.MarketResponses = make(map[string]MarketResponse)
	return &s
}

// Send simply sends message to the websocket client
func (s *Server) Send(conn *websocket.Conn, message []byte) {
	// send simple message
	conn.WriteMessage(websocket.TextMessage, message)
}

// SendWithWait sends message to the websocket client using wait group, allowing usage with goroutines
func (s *Server) SendWithWait(conn *websocket.Conn, message []byte, wg *sync.WaitGroup) {
	// send simple message
	conn.WriteMessage(websocket.TextMessage, message)

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
func (s *Server) HandleRequest(conn *websocket.Conn, config utils.PriceConfig, clientID string, message []byte) {
	slog.Info("recv: " + fmt.Sprint(message))
	var data ClientMessage
	err := json.Unmarshal(message, &data)
	if err != nil {
		// JSON parsing not successful
		return
	}
	reqTopic := strings.TrimSpace(strings.ToLower(data.Topic))
	reqType := strings.TrimSpace(strings.ToLower(data.Type))
	if reqType == "subscribe" {
		if reqTopic == MARKETS_TOPIC {
			msg := s.SubscribeMarkets(conn, clientID)
			server.Send(conn, msg)
		} else {
			msg := s.SubscribeCandles(conn, clientID, reqTopic, config)
			server.Send(conn, msg)
		}
	} else if reqType == "unsubscribe" {
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
func (s *Server) SubscribeMarkets(conn *websocket.Conn, clientID string) []byte {
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

func (s *Server) ScheduleUpdateMarketAndBroadcast(waitTime time.Duration, config utils.PriceConfig) {
	tickerUpdate := time.NewTicker(waitTime)
	s.UpdateMarketAndBroadcast(config)
	for {
		select {
		case <-tickerUpdate.C:
			// if no subscribers we update infrequently
			if len(server.Subscriptions[MARKETS_TOPIC]) == 0 &&
				rand.Float64() < 0.95 {
				slog.Info("UpdateMarketAndBroadcast: no subscribers")
				break
			}
			s.UpdateMarketAndBroadcast(config)
			fmt.Println("Market info data updated.")
		}
	}

}

func (s *Server) UpdateMarketAndBroadcast(config utils.PriceConfig) {
	s.UpdateMarketResponses(config)
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
	r := ServerResponse{Type: typeRes, Topic: MARKETS_TOPIC, Data: m}
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
func (s *Server) UpdateMarketResponses(config utils.PriceConfig) {
	feeds := config.ConfigFile.PriceFeeds
	nowUTCms := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	//yesterday:
	var anchorTime24hMs int64 = nowUTCms - 86400000
	for k := 0; k < len(feeds); k++ {
		s.updtMarketForSym(feeds[k].Symbol, anchorTime24hMs)
	}
	//triangulations:
	triang := config.ConfigFile.Triangulations
	for k := 0; k < len(triang); k++ {
		s.updtMarketForSym(triang[k].Target, anchorTime24hMs)
	}
}

func (s *Server) updtMarketForSym(sym string, anchorTime24hMs int64) error {
	m, err := builder.GetMarketInfo(s.RedisTSClient.Ctx, s.RedisTSClient.Client, sym)
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
	if err != nil {
		px24 = nil
	} else {
		ret = px.Value/px24[0].Value - 1
		scale := float64(px.Timestamp-px24[0].Timestamp) / float64(d)
		ret = ret * scale
	}

	var mr = MarketResponse{
		Sym:           sym,
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
func (s *Server) SubscribeCandles(conn *websocket.Conn, clientID string, topic string, config utils.PriceConfig) []byte {
	if !isValidCandleTopic(topic) {
		return errorResponse("subscribe", topic, "usage: symbol:period")
	}
	sym, period, isFound := strings.Cut(topic, ":")
	if !isFound {
		// usage: symbol:period
		return errorResponse("subscribe", topic, "usage: symbol:period")
	}
	if !config.IsSymbolAvailable(sym) {
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

func isValidCandleTopic(topic string) bool {
	pattern := "^[a-zA-Z]+-[a-zA-Z]+:[0-9]+[hmd]$" // Regular expression for candle topics
	regex, _ := regexp.Compile(pattern)
	return regex.MatchString(topic)
}

// form initial response for candle subscription (e.g., eth-usd:5m)
func (s *Server) candleResponse(sym string, p utils.CandlePeriod) []byte {
	slog.Info("Subscription for symbol " + sym + " Period " + fmt.Sprint(p.TimeMs/60000) + "m")
	data := GetInitialCandles(s.RedisTSClient, sym, p)
	topic := sym + ":" + p.Name
	res := ServerResponse{Type: "subscribe", Topic: topic, Data: data}
	jsonData, err := json.Marshal(res)
	if err != nil {
		slog.Error("candle res")
	}
	return jsonData
}

func errorResponse(reqType string, reqTopic string, msg string) []byte {

	e := ErrorResponse{Error: msg}
	res := ServerResponse{Type: reqType, Topic: reqTopic, Data: e}
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
		slog.Info("REDIS received message:" + msg.Payload)
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
				s.LastCandles[key] = &builder.OhlcData{}
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
				lastCandle.Time = builder.ConvertTimestampToISO8601(nextTs)
			} else {
				// update existing candle
				lastCandle.C = pxLast.Value
				lastCandle.H = math.Max(pxLast.Value, lastCandle.H)
				lastCandle.L = math.Min(pxLast.Value, lastCandle.L)
			}
			// update subscribers
			clients := server.Subscriptions[key]
			r := ServerResponse{Type: "update", Topic: key, Data: lastCandle}
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
