package wscandle

import (
	"context"
	"d8x-candles/src/builder"
	"d8x-candles/src/utils"
	"flag"
	"log/slog"
	"net/http"
	"time"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	// time to read the next client's pong message
	pongWait = 60 * time.Second
	// time period to send pings to client
	pingPeriod = (pongWait * 9) / 10
	// time allowed to write a message to client
	writeWait = 10 * time.Second
	// max message size allowed
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{}

// Initialize server with empty subscription
var server = &Server{
	Subscriptions: make(Subscriptions),
	LastCandles:   make(map[string]builder.OhlcData),
}
var config utils.PriceConfig
var redisClient *redis.Client
var redisTSClient *redistimeseries.Client
var ctx context.Context

func StartWSServer(config_ utils.PriceConfig, REDIS_ADDR string, REDIS_PW string) {
	flag.Parse()
	config = config_

	// Redis connection
	redisClient = redis.NewClient(&redis.Options{
		Addr:     REDIS_ADDR,
		Password: REDIS_PW,
		DB:       0,
	})
	redisTSClient = redistimeseries.NewClient(REDIS_ADDR, "client", &REDIS_PW)
	ctx = context.Background()
	subscriber := redisClient.Subscribe(ctx, "px_update")
	go server.SubscribePxUpdate(subscriber)

	http.HandleFunc("/ws", HandleWs)
	slog.Info("Listening on localhost:8080/ws")
	slog.Error(http.ListenAndServe(*addr, nil).Error())
}

var addr = flag.String("addr", "localhost:8080", "http service address")

func HandleWs(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Info("upgrade:", err)
		return
	}
	defer c.Close()
	// create new client id
	clientID := uuid.New().String()

	//log new client
	slog.Info("Server: new client connected, ID is %s", clientID)

	// create channel to signal client health
	done := make(chan struct{})

	go writePump(c, clientID, done)
	readPump(c, clientID, done)
}

// readPump process incoming messages and set the settings
func readPump(conn *websocket.Conn, clientID string, done chan<- struct{}) {
	// set limit, deadline to read & pong handler
	conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// message handling
	for {
		// read incoming message
		_, msg, err := conn.ReadMessage()
		// if error occured
		if err != nil {
			// remove from the client
			server.RemoveClient(clientID)
			// set health status to unhealthy by closing channel
			close(done)
			// stop process
			break
		}

		// if no error, process incoming message
		server.HandleRequest(conn, config, clientID, msg)
	}
}

// writePump sends ping to the client
func writePump(conn *websocket.Conn, clientID string, done <-chan struct{}) {
	// create ping ticker
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// send ping message
			err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeWait))
			if err != nil {
				// if error sending ping, remove this client from the server
				server.RemoveClient(clientID)
				// stop sending ping
				return
			}
		case <-done:
			// if process is done, stop sending ping
			return
		}
	}
}
