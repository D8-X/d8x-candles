package pythclient

import (
	"context"
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"strconv"
	"strings"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
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

type PriceUpdateResponse struct {
	Type      string `json:"type"`
	PriceFeed struct {
		ID    string `json:"id"`
		Price struct {
			Price       string `json:"price"`
			Conf        string `json:"conf"`
			Expo        int    `json:"expo"`
			PublishTime int64  `json:"publish_time"`
		} `json:"price"`
		EMAPrice struct {
			Price       string `json:"price"`
			Conf        string `json:"conf"`
			Expo        int    `json:"expo"`
			PublishTime int64  `json:"publish_time"`
		} `json:"ema_price"`
	} `json:"price_feed"`
}

type PriceMeta struct {
	AffectedTriang map[string][]string
	Triangulations map[string][]string
	RedisClient    *redis.Client
	RedisTSClient  *redistimeseries.Client
	Ctx            context.Context
}

// symMap maps pyth ids to internal symbol (btc-usd)
func StreamWs(config utils.PriceConfig, REDIS_ADDR string, REDIS_PW string) error {
	symMap := config.ExtractPythIdToSymbolMap()
	wsUrl := config.PythPriceWSEndpoint
	slog.Info("Using wsUrl=" + wsUrl)
	// keep track of latest price
	var lastPx = make(map[string]float64, len(symMap))
	var meta PriceMeta
	meta.AffectedTriang = config.ExtractSymbolToTriangTarget()
	meta.Triangulations = config.ExtractTriangulationMap()
	meta.RedisClient = redis.NewClient(&redis.Options{
		Addr:     REDIS_ADDR,
		Password: REDIS_PW,
		DB:       0,
	})
	meta.RedisTSClient = redistimeseries.NewClient(REDIS_ADDR, "client", &REDIS_PW)
	meta.Ctx = context.Background()
	var ids = make([]string, len(symMap))
	k := 0
	for id, _ := range symMap {
		ids[k] = "0x" + id
		k++
	}
	wsUrl = strings.TrimPrefix(wsUrl, "wss://")
	wsUrl, pathUrl, _ := strings.Cut(wsUrl, "/")
	u := url.URL{Scheme: "wss", Host: wsUrl, Path: pathUrl}
	slog.Info("Connecting to " + u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		slog.Error("Dial:", err)
	}
	defer c.Close()

	// Construct and send the subscribe request
	subscribeReq := SubscribeRequest{
		Type: "subscribe",
		IDs:  ids,
	}
	if err := c.WriteJSON(subscribeReq); err != nil {
		slog.Error("WriteJSON:" + err.Error())
	}

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			slog.Info("Read:" + err.Error())
			return err
		}
		var resp map[string]interface{}
		if err := json.Unmarshal(message, &resp); err != nil {
			slog.Info("JSON Unmarshal:" + err.Error())
			continue
		}
		switch resp["type"] {
		case "response":
			continue //{"type":"response","status":"success"}
		case "ping":
			{
				slog.Error("ping")
				panic("ping")
			}
		case "price_update":
			break
		default:
			continue
		}
		var pxResp PriceUpdateResponse
		if err := json.Unmarshal(message, &pxResp); err != nil {
			slog.Info("JSON Unmarshal:" + err.Error())
			continue
		}

		if pxResp.Type == "price_update" {
			// Handle price update response
			onPriceUpdate(pxResp, symMap[pxResp.PriceFeed.ID], lastPx, meta)
		}
	}
}

func onPriceUpdate(pxResp PriceUpdateResponse, sym string, lastPx map[string]float64, meta PriceMeta) {
	if sym == "" {
		return
	}
	px := calcPrice(pxResp)
	if px-lastPx[sym] == 0 {
		// price identical
		return
	}
	lastPx[sym] = px
	meta.RedisTSClient.Add(sym, pxResp.PriceFeed.Price.PublishTime, px)

	slog.Info("Received price update: " + sym + " price=" + fmt.Sprint(px) + fmt.Sprint(pxResp.PriceFeed))

	pubMsg := sym
	// triangulations
	targetSymbols := meta.AffectedTriang[sym]
	for _, tsym := range targetSymbols {
		// TODO: if market closed for any of the items in the triangulation,
		// we should not publish an update
		pxTriang := utils.Triangulate(meta.Triangulations[tsym], lastPx)
		lastPx[tsym] = pxTriang
		pubMsg += ";" + tsym
		meta.RedisTSClient.Add(tsym, pxResp.PriceFeed.Price.PublishTime, pxTriang)
		slog.Info("-- triangulation price update: " + tsym + " price=" + fmt.Sprint(pxTriang))
	}
	// publish updates to listeners
	err := meta.RedisClient.Publish(meta.Ctx, "px_update", pubMsg).Err()
	if err != nil {
		slog.Error("Redis Pub" + err.Error())
	}
}

// calculate floating point price from 'price' and 'expo'
func calcPrice(pxResp PriceUpdateResponse) float64 {
	x, err := strconv.Atoi(pxResp.PriceFeed.Price.Price)
	if err != nil {
		slog.Error("onPriceUpdate error" + err.Error())
		return 0
	}
	pw := float64(pxResp.PriceFeed.Price.Expo)
	px := float64(x) * math.Pow(10, pw)
	return px
}

/* stream prices from HTTP
func Stream(baseUrl string, symMap map[string]string) error {
	//https://benchmarks.pyth.network/v1/shims/tradingview/streaming
	resp, err := http.Get(baseUrl + "/v1/shims/tradingview/streaming")
	if err != nil {
		return err
	}
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			slog.Error(err.Error())
			continue
		}
		var d PythStream
		err = json.Unmarshal(line, &d)
		if err != nil {
			slog.Error(err.Error())
			continue
		}
		sym := symMap[d.Id]
		if sym == "" {
			// not part of the symbols we're interested in
			continue
		}
		slog.Info(string(line))

	}
}
*/