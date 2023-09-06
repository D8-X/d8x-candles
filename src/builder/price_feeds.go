package builder

import (
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type MarketHours struct {
	IsOpen    bool   `json:"is_open"`
	NextOpen  *int64 `json:"next_open"`
	NextClose *int64 `json:"next_close"`
}

type PriceFeedApiResponse struct {
	ID          string            `json:"id"`
	MarketHours MarketHours       `json:"market_hours"`
	Attributes  map[string]string `json:"attributes"`
}

func (p *PythHistoryAPI) BuildPriceFeedInfo(config *utils.PriceConfig) {
	// process base price feeds (no triangulation)
	f := config.ConfigFile.PriceFeeds
	for k := 0; k < len(f); k++ {
		sym := f[k].Symbol
		id := f[k].Id
		p.QueryPriceFeedInfo(sym, id)
	}
}

func (p *PythHistoryAPI) QueryPriceFeedInfo(sym string, id string) {
	const endpoint = "/v1/price_feeds/"
	url := strings.TrimSuffix(p.BaseUrl, "/") + endpoint + id
	// Send a GET request
	var response *http.Response
	var err error
	for {
		if p.TokenBucket.Take() {
			response, err = http.Get(url)
			if err != nil {
				slog.Error("Error making GET request:" + err.Error())
				return
			}
			break
		}
		slog.Info("too many requests [PriceFeed], slowing down for " + sym)
		time.Sleep(time.Duration(rand.Intn(25)) * time.Millisecond)
	}
	defer response.Body.Close()
	// Check response status code
	if response.StatusCode != http.StatusOK {
		slog.Error("unexpected status code[PriceFeed]: " + fmt.Sprintf("%d", response.StatusCode))
		return
	}
	// Read the response body
	var apiResponse []PriceFeedApiResponse
	err = json.NewDecoder(response.Body).Decode(&apiResponse)
	if err != nil {
		slog.Error("Error parsing GET request:" + err.Error())
		return
	}
	// check whether id provided is indeed for the symbol we aim to store
	symSource := strings.ToLower(apiResponse[0].Attributes["generic_symbol"])
	if symSource != strings.ReplaceAll(sym, "-", "") {
		slog.Error("Error: price_feeds GET id is for " + symSource +
			" but symbol " + sym)
		return
	}
	p.setMarketHours(sym, apiResponse[0].MarketHours)
}

func (p *PythHistoryAPI) setMarketHours(ticker string, mh MarketHours) error {
	c := *p.RedisClient.Client
	var nxto, nxtc string
	if mh.NextOpen == nil {
		nxto = "null"
	} else {
		nxto = strconv.FormatInt(*mh.NextOpen, 10)
	}
	if mh.NextClose == nil {
		nxtc = "null"
	} else {
		nxtc = strconv.FormatInt(*mh.NextClose, 10)
	}
	c.Do(p.RedisClient.Ctx, c.B().Hset().Key(ticker+":mkt_hours").
		FieldValue().FieldValue("is_open", strconv.FormatBool(mh.IsOpen)).
		FieldValue("nxt_open", nxto).
		FieldValue("nxt_close", nxtc).Build())
	return nil
}

func (p *PythHistoryAPI) GetMarketHours(ticker string) (MarketHours, error) {
	c := *p.RedisClient.Client
	hm, err := c.Do(p.RedisClient.Ctx, c.B().Hgetall().Key(ticker+":mkt_hours").Build()).AsStrMap()
	if err != nil {
		return MarketHours{}, err
	}
	isOpen, _ := strconv.ParseBool(hm["is_open"])
	nxtOpen, _ := strconv.ParseInt(hm["nxt_open"], 10, 64)
	nxtClose, _ := strconv.ParseInt(hm["nxt_open"], 10, 64)
	var mh = MarketHours{
		IsOpen:    isOpen,
		NextOpen:  &nxtOpen,
		NextClose: &nxtClose,
	}
	return mh, nil
}
