package wscandle

import (
	"d8x-candles/src/builder"
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	redis "github.com/redis/go-redis/v9"
)

// Subscriptions is a type for each string of topic and the clients that subscribe to it
type Subscriptions map[string]Clients

// Clients is a type that describe the clients' ID and their connection
type Clients map[string]*websocket.Conn

// Server is the struct to handle the Server functions & manage the Subscriptions
type Server struct {
	Subscriptions Subscriptions
	LastCandles   map[string]*builder.OhlcData //symbol:period->OHLC
	RedisTSClient *utils.RueidisClient
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
		if reqTopic == "markets" {
			msg := s.SubscribeMarkets(conn, clientID)
			server.Send(conn, msg)
		} else {
			msg := s.SubscribeCandles(conn, clientID, reqTopic, config)
			server.Send(conn, msg)
		}
	} else {
		// unsubscribe
		if reqTopic == "markets" {
			delete(s.Subscriptions[reqTopic], clientID)
		} else {
			server.UnsubscribeCandles(clientID, reqTopic)
		}
	}
}

func (s *Server) UnsubscribeCandles(clientID string, topic string) {
	// if topic exists, check the client map
	if _, exist := s.Subscriptions[topic]; exist {
		client := s.Subscriptions[topic]
		// remove the client from the topic's client map
		delete(client, clientID)
	}
}

func (s *Server) SubscribeMarkets(conn *websocket.Conn, clientID string) []byte {
	client := s.Subscriptions["markets"]
	// if client already subbed, stop the process
	if _, subbed := client[clientID]; subbed {
		return []byte{}
	}
	// if not subbed, add to client map
	client[clientID] = conn
	return []byte{}
}

func (s *Server) SubscribeCandles(conn *websocket.Conn, clientID string, topic string, config utils.PriceConfig) []byte {
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

func (s *Server) SubscribePxUpdate(sub *redis.PubSub) {
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

func (s *Server) candleUpdates(symbols []string) {

	var wg sync.WaitGroup
	for _, sym := range symbols {
		c := (*s.RedisTSClient)
		pxLast, err := c.Get(sym)

		if err != nil {
			slog.Error(fmt.Sprintf("Error parsing date:%v", err))
			return
		}
		for _, prd := range config.CandlePeriodsMs {
			key := sym + ":" + prd.Name
			lastCandle := s.LastCandles[key]
			if lastCandle == nil {
				s.LastCandles[key] = &builder.OhlcData{}
				lastCandle = s.LastCandles[key]
			}
			if pxLast.Timestamp > lastCandle.StartTsMs+int64(prd.TimeMs) {
				// new candle
				lastCandle.O = pxLast.Value
				lastCandle.H = pxLast.Value
				lastCandle.L = pxLast.Value
				lastCandle.C = pxLast.Value
				nextTs := (pxLast.Timestamp / int64(prd.TimeMs)) * int64(prd.TimeMs)
				lastCandle.StartTsMs = nextTs
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
				slog.Error("forming lastCandle update")
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
