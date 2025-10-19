package wscandle

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"d8x-candles/src/utils"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/rueidis"
)

const (
	// time to read the next client's pong message
	PONG_WAIT = 15 * time.Second
	// time period to send pings to client
	PING_PERIOD_SEC = 30 * time.Second
	// time allowed to write a message to client
	WRITE_WAIT = 5 * time.Second
	// max message size allowed
	MAX_MSG_SIZE = 1024
)

type WsCandle struct {
	Upgrader        websocket.Upgrader
	Server          *Server
	CandlePeriodsMs map[string]utils.CandlePeriod
	Client          *rueidis.Client
	WsAddr          string
}

func NewWsCandle(cndlPeriodsMs map[string]utils.CandlePeriod, WS_ADDR string, REDIS_ADDR string, REDIS_PW string, RedisDb int) (*WsCandle, error) {
	var ws WsCandle
	ws.WsAddr = WS_ADDR
	ws.Upgrader = websocket.Upgrader{}

	ws.CandlePeriodsMs = cndlPeriodsMs
	// Initialize server with empty subscription
	ws.Server = NewServer()
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{REDIS_ADDR}, Password: REDIS_PW})
	if err != nil {
		return nil, err
	}
	ws.Server.RedisTSClient = &client
	return &ws, nil
}

func (ws *WsCandle) StartWSServer() error {
	errChan := make(chan error, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Cancel all goroutines when an error occurs

	go ws.monitorPxUpdates(ctx, errChan)
	go ws.Server.ScheduleUpdateMarketAndBroadcast(ctx, 1*time.Minute)
	go ws.listen(ctx, errChan)
	err := <-errChan
	return err
}

func (ws *WsCandle) monitorPxUpdates(ctx context.Context, errChan chan error) {
	ctxSub, cancelSub := context.WithCancel(context.Background())
	go ws.subscribePriceUpdate(ctxSub, errChan)
	ticker := time.NewTicker(1 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			cancelSub()
			return
		case <-ticker.C:
			if ws.Server.IsPxSubscriptionExpired() {
				slog.Warn("Last update too old, restarting subscription")
				cancelSub()
				// Start a new subscription
				ctxSub, cancelSub = context.WithCancel(context.Background())
				go ws.subscribePriceUpdate(ctxSub, errChan)
			}
		case err := <-errChan:
			slog.Error("terminating monitorPxUpdates", "error", err)
			cancelSub() // Clean up the subscription
			return
		}
	}
}

func (ws *WsCandle) listen(ctx context.Context, errChan chan error) {
	// register websocket handler
	http.HandleFunc("/ws", ws.HandleWs)
	// create http server
	server := &http.Server{
		Addr:    ws.WsAddr,
		Handler: http.DefaultServeMux, // Use your desired handler
	}

	// Start the server in a separate goroutine
	go func() {
		slog.Info("Listening on " + ws.WsAddr + "/ws")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("HTTP server failed: %w", err)
		}
	}()

	// Wait for either context cancellation or server failure
	select {
	case <-ctx.Done():
		slog.Info("Context canceled; shutting down HTTP server")
		server.Shutdown(context.Background())
	case <-errChan:
		slog.Info("HTTP server stopped; initiating shutdown")
	}
}

// subscribePriceUpdate redis pub/sub utils.RDS_TICKER_REQUEST
func (ws *WsCandle) subscribePriceUpdate(ctx context.Context, errChan chan error) {
	c := *ws.Server.RedisTSClient
	cmd := c.B().Subscribe().Channel(utils.RDS_PRICE_UPDATE_MSG).Build()
	err := c.Receive(ctx, cmd,
		func(msg rueidis.PubSubMessage) {
			ws.Server.HandlePxUpdateFromRedis(msg, ws.CandlePeriodsMs)
		})
	if err != nil && err != ctx.Err() {
		slog.Warn("subscribePriceUpdate", "error", err)
		errChan <- err
	}
}

// NewClient creates a client and adds the client to the Server-struct
func (ws *WsCandle) NewClient(clientID string, wsConn *websocket.Conn) *ClientConn {
	client := &ClientConn{
		conn: wsConn,
		send: make(chan []byte, 256), // note the buffer size
		subs: make(map[string]struct{}),
	}
	client.removeFunc = func() {
		ws.Server.RemoveClient(clientID, client)
	}
	ws.Server.AddClient(clientID, client)
	return client
}

func (ws *WsCandle) HandleWs(w http.ResponseWriter, r *http.Request) {
	ws.Upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	c, err := ws.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Info("upgrade:" + err.Error())
		return
	}
	// create new client id
	clientID := uuid.New().String()

	// log new client
	slog.Info("Server: new client connected, ID is " + clientID)
	client := ws.NewClient(clientID, c)
	go client.writePump()
	go client.readPump(ws, clientID)
}

// readPump process incoming messages and set the settings
func (c *ClientConn) readPump(ws *WsCandle, clientID string) {
	defer func() {
		c.conn.Close()
		if c.removeFunc != nil {
			c.removeFunc()
		}
	}()
	// set limit, deadline to read & pong handler
	c.conn.SetReadLimit(MAX_MSG_SIZE)
	c.conn.SetReadDeadline(time.Now().Add(PING_PERIOD_SEC + PONG_WAIT))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(PING_PERIOD_SEC + PONG_WAIT))
		return nil
	})
	c.conn.SetPingHandler(func(appData string) error {
		c.conn.SetReadDeadline(time.Now().Add(PING_PERIOD_SEC + PONG_WAIT))
		// send pong
		return c.conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(WRITE_WAIT))
	})
	// message handling
	for {
		// read incoming message
		_, msg, err := c.conn.ReadMessage()
		// if error occured
		if err != nil {
			slog.Info("readPump error", "clientId", clientID, "error", err)
			break
		}
		// if no error, process incoming message
		ws.Server.HandleRequest(c, ws.CandlePeriodsMs, clientID, msg)
	}
}

// writePump sends ping and messages in the send channel to the client
func (c *ClientConn) writePump() {
	// create ping ticker
	ticker := time.NewTicker(PING_PERIOD_SEC)
	defer func() {
		ticker.Stop()
		c.conn.Close()
		if c.removeFunc != nil {
			c.removeFunc()
		}
	}()
	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				// Channel closed, close connection
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			// Send ping
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
