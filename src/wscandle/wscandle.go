package wscandle

import (
	"context"
	"d8x-candles/src/utils"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
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

func (ws *WsCandle) HandleWs(w http.ResponseWriter, r *http.Request) {
	ws.Upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	c, err := ws.Upgrader.Upgrade(w, r, nil)
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

	go ws.writePump(c, clientID, done)
	ws.readPump(&conn, clientID, done)
}

// readPump process incoming messages and set the settings
func (ws *WsCandle) readPump(conn *ClientConn, clientID string, done chan<- struct{}) {
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
			ws.Server.RemoveClient(clientID)
			// set health status to unhealthy by closing channel
			close(done)
			// stop process
			break
		}

		// if no error, process incoming message
		ws.Server.HandleRequest(conn, ws.CandlePeriodsMs, clientID, msg)
	}
}

// writePump sends ping to the client
func (ws *WsCandle) writePump(conn *websocket.Conn, clientID string, done <-chan struct{}) {
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
				ws.Server.RemoveClient(clientID)
				// stop sending ping
				return
			}
		case <-done:
			// if process is done, stop sending ping
			return
		}
	}
}
