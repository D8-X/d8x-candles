package wscandle

import (
	"context"
	"d8x-candles/src/utils"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"
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
var server = NewServer()
var config utils.SymbolManager
var redisClient *redis.Client

func StartWSServer(config_ utils.SymbolManager, WS_ADDR string, REDIS_ADDR string, REDIS_PW string, RedisDb int) error {
	config = config_

	// Redis connection
	redisClient = redis.NewClient(&redis.Options{
		Addr:     REDIS_ADDR,
		Password: REDIS_PW,
		DB:       RedisDb,
	})
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{REDIS_ADDR}, Password: REDIS_PW})
	if err != nil {
		return err
	}
	ctx := context.Background()
	server.RedisTSClient = &utils.RueidisClient{
		Client: &client,
		Ctx:    ctx,
	}
	ctx = context.Background()
	subscriber := redisClient.Subscribe(ctx, utils.PRICE_UPDATE_MSG)
	go server.SubscribePxUpdate(subscriber, ctx)
	go server.ScheduleUpdateMarketAndBroadcast(5*time.Second, config)
	http.HandleFunc("/ws", HandleWs)
	slog.Info("Listening on " + WS_ADDR + "/ws")
	slog.Error(http.ListenAndServe(WS_ADDR, nil).Error())
	return nil
}

func HandleWs(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Info("upgrade:" + err.Error())
		return
	}
	defer c.Close()
	// create new client id
	clientID := uuid.New().String()

	//log new client
	slog.Info("Server: new client connected, ID is " + clientID)
	conn := ClientConn{Conn: c}
	// create channel to signal client health
	done := make(chan struct{})

	go writePump(c, clientID, done)
	readPump(&conn, clientID, done)
}

// readPump process incoming messages and set the settings
func readPump(conn *ClientConn, clientID string, done chan<- struct{}) {
	// set limit, deadline to read & pong handler
	conn.Conn.SetReadLimit(maxMessageSize)
	conn.Conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.Conn.SetPongHandler(func(string) error {
		conn.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// message handling
	for {
		// read incoming message
		_, msg, err := conn.Conn.ReadMessage()
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
