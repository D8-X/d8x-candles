package pythclient

import (
	"bufio"
	"context"
	"d8x-candles/src/builder"
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

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
	}
	// clean ticker availability
	cl := *redisTSClient.Client
	cl.Do(context.Background(), cl.B().Del().Key(utils.AVAIL_TICKER_SET).Build())

	slog.Info("Building price history...")
	ph.BuildHistory()
	go ph.ScheduleMktInfoUpdate(15 * time.Minute)
	go ph.ScheduleCompaction(15 * time.Minute)

	err = streamHttp(symMngr.ConfigFile.PythPriceEndpoints, symMap, &ph)
	if err != nil {
		return err
	}
	/*
		// Construct and send the subscribe request
		subscribeReq := SubscribeRequest{
			Type: "subscribe",
			IDs:  ids,
		}

		go ph.SubscribeTickerRequest()
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				slog.Info("Pyth Price Service Websocket Read:" + err.Error())
				slog.Info("Reconnecting...")
				if err = streamHttp(symMngr.ConfigFile.PythPriceEndpoints, ids); err != nil {
					return err
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
	*/
	return nil
}

func streamHttp(endpoints []string, symMap map[string]string, ph *builder.PythHistoryAPI) error {
	shuffleSlice(endpoints)

	//https://hermes.pyth.network/docs/#/
	postfix := "/v2/updates/price/stream?parsed=true&allow_unordered=true&benchmarks_only=true&encoding=base64"
	for id := range symMap {
		postfix += "&ids[]=" + "0x" + id
	}

	var err error
	var resp *http.Response
	for _, url := range endpoints {
		url, _ := strings.CutSuffix(url, "/")
		url += postfix
		slog.Info("Connecting to " + url)
		resp, err = http.Get(url)
		if err != nil {
			slog.Info("streaming not successful:" + err.Error())
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("received non-200 status code %d", resp.StatusCode)
		}
	}
	if err != nil {
		return err
	}

	// keep track of latest price for triangulations
	var lastPx = make(map[string]float64, len(symMap))

	// Create a buffered reader to read the response body as a stream
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err.Error() == "EOF" {
				// End of stream
				break
			}
			fmt.Fprintf(os.Stderr, "error reading stream: %v\n", err)
		}
		if len(line) > 3 && string(line[0:3]) == ":No" || len(line) < 3 {
			continue
		}
		var response utils.PythStreamData
		jsonData := string(line[5:])
		err = json.Unmarshal([]byte(jsonData), &response)
		if err != nil {
			fmt.Println("Error unmarshalling JSON:", err)
			continue
		}
		// Process the data
		for _, parsed := range response.Parsed {
			ph.OnPriceUpdate(parsed.Price, parsed.Id, symMap[parsed.Id], lastPx)
		}
	}
	return nil
}

func shuffleSlice(slice []string) {
	n := len(slice)
	for i := 0; i < n-1; i++ {
		j := rand.Intn(n)
		slice[i], slice[j] = slice[j], slice[i]
	}
}
