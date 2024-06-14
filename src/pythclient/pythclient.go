package pythclient

import (
	"context"
	"d8x-candles/src/builder"
	"d8x-candles/src/utils"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/rueidis"
)

type PythStream struct {
	Id string  `json:"id"`
	P  float64 `json:"p"`
	T  uint32  `json:"t"`
	F  string  `json:"f"`
	S  int8    `json:"s"`
}

type SubscribeRequest struct {
	Type string   `json:"type"`
	IDs  []string `json:"ids"`
}

// symMap maps pyth ids to internal symbol (btc-usd)
func StreamWs(symMngr *utils.SymbolManager, REDIS_ADDR string, REDIS_PW string) error {
	symMap := symMngr.PythIdToSym

	// keep track of latest price
	var lastPx = make(map[string]float64, len(symMap))
	fmt.Print("REDIS ADDR = ", REDIS_ADDR)
	fmt.Print("REDIS_PW=", REDIS_PW)

	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{REDIS_ADDR}, Password: REDIS_PW})
	if err != nil {
		return err
	}
	redisTSClient := &utils.RueidisClient{
		Client: &client,
		Ctx:    context.Background(),
	}
	//https://docs.pyth.network/benchmarks/rate-limits
	capacity := 30
	refillRate := 9.0
	tb := builder.NewTokenBucket(capacity, refillRate)
	ph := builder.PythHistoryAPI{
		BaseUrl:     symMngr.ConfigFile.PythAPIEndpoint,
		RedisClient: redisTSClient,
		TokenBucket: tb,
		SymbolMngr:  symMngr,
		MsgCount:    make(map[string]int),
	}
	// clean ticker availability
	cl := *redisTSClient.Client
	cl.Do(context.Background(), cl.B().Del().Key(utils.AVAIL_TICKER_SET).Build())

	slog.Info("Building price history...")
	ph.BuildHistory()
	go ph.ScheduleMktInfoUpdate(15 * time.Minute)
	go ph.ScheduleCompaction(15 * time.Minute)
	var ids = make([]string, len(symMap))
	k := 0
	for id := range symMap {
		ids[k] = "0x" + id
		k++
	}

	c, err := connectToWebsocket(symMngr.ConfigFile.PythPriceWSEndpoints)
	if err != nil {
		return err
	}
	defer c.Close()

	// Construct and send the subscribe request
	subscribeReq := SubscribeRequest{
		Type: "subscribe",
		IDs:  ids,
	}
	if err := c.WriteJSON(subscribeReq); err != nil {
		return errors.New("WriteJSON:" + err.Error())
	}

	go ph.SubscribeTickerRequest()
	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			slog.Info("Pyth Price Service Websocket Read:" + err.Error())
			slog.Info("Reconnecting...")
			if c, err = connectToWebsocket(symMngr.ConfigFile.PythPriceWSEndpoints); err != nil {
				return err
			}
			if err := c.WriteJSON(subscribeReq); err != nil {
				return errors.New("WriteJSON:" + err.Error())
			}
		}
		var resp map[string]interface{}
		if err := json.Unmarshal(message, &resp); err != nil {
			slog.Info("JSON Unmarshal:" + err.Error())
			continue
		}
		switch resp["type"] {
		case "response":
			if resp["status"] == "success" {
				//{"type":"response","status":"success"}
				continue
			}
			msg := fmt.Sprintf("%s : %s", resp["status"], resp["error"])
			slog.Error("Pyth response " + msg)
			break
		case "price_update":
			break
		default:
			continue
		}
		var pxResp utils.PriceUpdateResponse
		if err := json.Unmarshal(message, &pxResp); err != nil {
			slog.Info("JSON Unmarshal:" + err.Error())
			continue
		}

		if pxResp.Type == "price_update" {
			// Handle price update response
			ph.OnPriceUpdate(pxResp, symMap[pxResp.PriceFeed.ID], lastPx)
		}
	}
}

func connectToWebsocket(endpoints []string) (*websocket.Conn, error) {
	shuffleSlice(endpoints)
	var c *websocket.Conn
	var err error
	for _, wsUrl := range endpoints {
		slog.Info("Using wsUrl=" + wsUrl)
		wsUrl = strings.TrimPrefix(wsUrl, "wss://")
		wsUrl, pathUrl, _ := strings.Cut(wsUrl, "/")
		u := url.URL{Scheme: "wss", Host: wsUrl, Path: pathUrl}
		slog.Info("Connecting to " + u.String())
		c, _, err = websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			slog.Info("Dial not successful:" + err.Error())
			continue
		}
		return c, nil
	}
	return nil, errors.New("Could not connect to any price-service websocket endpoint")
}

func shuffleSlice(slice []string) {
	n := len(slice)
	for i := 0; i < n-1; i++ {
		j := rand.Intn(n)
		slice[i], slice[j] = slice[j], slice[i]
	}
}
