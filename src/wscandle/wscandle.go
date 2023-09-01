package wscandle

import (
	"encoding/json"
	"flag"
	"log"
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

func StartWSServer() {
	flag.Parse()
	http.HandleFunc("/ws", wsMarkets)
	slog.Info("Listening on localhost:8080/ws")
	slog.Error(http.ListenAndServe(*addr, nil).Error())
}

var addr = flag.String("addr", "localhost:8080", "http service address")

func wsMarkets(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Info("upgrade:", err)
		return
	}
	defer c.Close()
	for {
		mt, message, err := c.ReadMessage()
		if err != nil {
			slog.Info("read:", err)
			break
		}
		resJSON := handleRequest(message)
		err = c.WriteMessage(mt, resJSON)
		if err != nil {
			slog.Info("write:", err)
			continue
		}
	}

}

func handleRequest(message []byte) []byte {
	log.Printf("recv: %s", message)
	var data map[string]interface{}
	err := json.Unmarshal(message, &data)
	if err != nil {
		// JSON parsing not successful
		return []byte{}
	}
	switch data["type"] {
	case "subscribe-markets":
		return subscribeMarkets()
	case "subscribe":
		return subscribeCandles()
	case "ping":
		return pong()
	default:
		return []byte{}
	}
}

func pong() []byte {
	response := map[string]interface{}{
		"type": "ping",
		"msg":  "pong",
	}
	responseJSON, _ := json.Marshal(response)
	return responseJSON
}

func subscribeMarkets() []byte {
	return []byte{}
}

func subscribeCandles() []byte {
	return []byte{}
}
