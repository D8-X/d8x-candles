package wscandle

import (
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/gorilla/websocket"
)

// Subscriptions is a type for each string of topic and the clients that subscribe to it
type Subscriptions map[string]Clients

// Server is the struct to handle the Server functions & manage the Subscriptions
type Server struct {
	Subscriptions Subscriptions
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

// Clients is a type that describe the clients' ID and their connection
type Clients map[string]*websocket.Conn

// Send simply sends message to the websocket client
func (s *Server) Send(conn *websocket.Conn, message []byte) {
	// send simple message
	conn.WriteMessage(websocket.TextMessage, message)
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
		return candleResponse(sym, p)
	}

	// if topic does not exist, create a new topic
	newClient := make(Clients)
	s.Subscriptions[topic] = newClient

	// add the client to the topic
	s.Subscriptions[topic][clientID] = conn
	return candleResponse(sym, p)
}

func candleResponse(sym string, p utils.CandlePeriod) []byte {
	slog.Info("Subscription for symbol " + sym + " Period " + fmt.Sprint(p.TimeMs/60000) + "m")
	data := []byte{}
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
