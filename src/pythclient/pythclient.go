package pythclient

import (
	"context"
	"d8x-candles/src/builder"
	"d8x-candles/src/utils"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
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
	RedisTSClient  *utils.RueidisClient
	Ctx            context.Context
}

// symMap maps pyth ids to internal symbol (btc-usd)
func StreamWs(config utils.PriceConfig, REDIS_ADDR string, REDIS_PW string, network string) error {
	symMap := config.PythIdToSym

	// keep track of latest price
	var lastPx = make(map[string]float64, len(symMap))
	var meta PriceMeta
	meta.AffectedTriang = config.SymToDependentTriang
	meta.Triangulations = config.SymToTriangPath
	fmt.Print("REDIS ADDR = ", REDIS_ADDR)
	fmt.Print("REDIS_PW=", REDIS_PW)
	meta.RedisClient = redis.NewClient(&redis.Options{
		Addr:     REDIS_ADDR,
		Password: REDIS_PW,
		DB:       0,
	})
	meta.Ctx = context.Background()
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{REDIS_ADDR}, Password: REDIS_PW})
	if err != nil {
		return err
	}
	meta.RedisTSClient = &utils.RueidisClient{
		Client: &client,
		Ctx:    meta.Ctx,
	}
	capacity := 30
	refillRate := 3.0 // 3 tokens per second
	tb := builder.NewTokenBucket(capacity, refillRate)
	ph := builder.PythHistoryAPI{
		BaseUrl:     config.ConfigFile.PythAPIEndpoint,
		RedisClient: meta.RedisTSClient,
		TokenBucket: tb,
	}
	slog.Info("Building price history...")
	buildHistory(meta.RedisTSClient, config, ph, network)
	go ph.ScheduleMktInfoUpdate(&config, 15*time.Minute)
	go ph.ScheduleCompaction(&config, 30*time.Minute)
	var ids = make([]string, len(symMap))
	k := 0
	for id := range symMap {
		ids[k] = "0x" + id
		k++
	}

	c, err := connectToWebsocket(config.ConfigFile.PythPriceWSEndpoints)
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
		slog.Error("WriteJSON:" + err.Error())
	}

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			slog.Info("Pyth Price Service Websocket Read:" + err.Error())
			return err
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
	builder.AddPriceObs(meta.RedisTSClient, sym, pxResp.PriceFeed.Price.PublishTime*1000, px)

	slog.Info("Received price update: " + sym + " price=" + fmt.Sprint(px) + fmt.Sprint(pxResp.PriceFeed))

	pubMsg := sym
	// triangulations
	targetSymbols := meta.AffectedTriang[sym]
	for _, tsym := range targetSymbols {
		// if market closed for any of the items in the triangulation,
		if isTriangulatedMarketClosed(tsym, meta.Triangulations[tsym], meta) {
			slog.Info("-- triangulation price update: " + tsym + " - market closed")
			continue
		}
		// we should not publish an update
		pxTriang := utils.Triangulate(meta.Triangulations[tsym], lastPx)
		if pxTriang == 0 {
			slog.Info("-- triangulation currently not possible: " + tsym)
			continue
		}
		lastPx[tsym] = pxTriang
		pubMsg += ";" + tsym
		builder.AddPriceObs(meta.RedisTSClient, tsym, pxResp.PriceFeed.Price.PublishTime*1000, pxTriang)
		slog.Info("-- triangulation price update: " + tsym + " price=" + fmt.Sprint(pxTriang))
	}
	// publish updates to listeners
	err := meta.RedisClient.Publish(meta.Ctx, "px_update", pubMsg).Err()
	if err != nil {
		slog.Error("Redis Pub" + err.Error())
	}
}

// check whether any of the symbols in the triangulation has a closed market
func isTriangulatedMarketClosed(tsym string, symbols []string, meta PriceMeta) bool {
	info, err := builder.GetMarketInfo(meta.Ctx, meta.RedisTSClient.Client, tsym)
	if err != nil {
		slog.Error("Error market-closed determination " + tsym + ":" + err.Error())
		return true
	}
	if !info.MarketHours.IsOpen {
		return true
	}
	for _, sym := range symbols {
		info, err := builder.GetMarketInfo(meta.Ctx, meta.RedisTSClient.Client, tsym)
		if err != nil {
			slog.Error("Error market-closed determination " + tsym + "(" + sym + "):" + err.Error())
			return true
		}
		if !info.MarketHours.IsOpen {
			return true
		}
	}
	return false
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

func buildHistory(client *utils.RueidisClient, config utils.PriceConfig, ph builder.PythHistoryAPI, network string) {

	pythSyms := make([]utils.SymbolPyth, len(config.ConfigFile.PriceFeeds))
	for k, feed := range config.ConfigFile.PriceFeeds {
		var sym utils.SymbolPyth
		if network == "testnet" {
			sym.New(feed.SymbolPyth, feed.IdVaaTest)
		} else {
			sym.New(feed.SymbolPyth, feed.Id)
		}

		pythSyms[k] = sym
	}
	slog.Info("-- building history from Pyth candles...")
	ph.PythDataToRedisPriceObs(pythSyms)
	slog.Info("-- triangulating history...")
	ph.CandlesToTriangulatedCandles(client, config)
}
