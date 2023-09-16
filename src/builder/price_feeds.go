package builder

import (
	"context"
	"d8x-candles/src/utils"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

type MarketHours struct {
	IsOpen    bool  `json:"is_open"`
	NextOpen  int64 `json:"next_open"`
	NextClose int64 `json:"next_close"`
}

type MarketInfo struct {
	MarketHours MarketHours
	AssetType   string `json:"assetType"`
}

type PriceFeedApiResponse struct {
	ID          string            `json:"id"`
	MarketHours MarketHours       `json:"market_hours"`
	Attributes  map[string]string `json:"attributes"`
}

// Runs FetchMktHours and schedules next runs
func (p *PythHistoryAPI) ScheduleMktInfoUpdate(config *utils.PriceConfig, updtInterval time.Duration) {
	p.FetchMktInfo(config)
	tickerUpdate := time.NewTicker(updtInterval)
	for {
		select {
		case <-tickerUpdate.C:
			slog.Info("Updating market info...")
			p.FetchMktInfo(config)
			fmt.Println("Market info updated.")
		}
	}
}

// Goes through all symbols in the config files, including triangulated ones,
// and fetches market hours (next open, next close). Stores in
// Redis
func (p *PythHistoryAPI) FetchMktInfo(config *utils.PriceConfig) {
	// process base price feeds (no triangulation)
	f := config.ConfigFile.PriceFeeds
	for k := 0; k < len(f); k++ {
		sym := f[k].Symbol
		asset := strings.ToLower(strings.Split(f[k].SymbolPyth, ".")[0])
		if asset == "crypto" {
			// crypto markets are always open, huray
			p.setMarketHours(sym, MarketHours{true, 0, 0}, "crypto")
			continue
		}
		id := f[k].Id
		p.QueryPriceFeedInfo(sym, id)
	}
	// construct info for triangulated price feeds, e.g. chf-usdc
	p.fetchTriangulatedMktInfo(config)
}

func (p *PythHistoryAPI) fetchTriangulatedMktInfo(config *utils.PriceConfig) {
	paths := config.SymToTriangPath
outerLoop:
	for symT, path := range paths {
		isOpen := true
		var nxtOpen, nxtClose int64 = 0, math.MaxInt64
		var assetType string = "crypto"
		for k := 1; k < len(path); k += 2 {
			m, err := p.GetMarketInfo(path[k])
			if m.AssetType != "crypto" {
				// dominant asset type for triangulations is
				// the non-crypto asset
				assetType = m.AssetType
			}
			if err != nil {
				slog.Error("Error triangulated feeds info " + symT + " at " + path[k])
				continue outerLoop
			}
			isOpen = isOpen && m.MarketHours.IsOpen
			if m.MarketHours.NextOpen != 0 {
				if m.MarketHours.NextOpen > nxtOpen {
					nxtOpen = m.MarketHours.NextOpen
				}
			}
			if m.MarketHours.NextClose != 0 {
				if m.MarketHours.NextClose < nxtClose {
					nxtClose = m.MarketHours.NextClose
				}
			}
		}

		p.setMarketHours(symT, MarketHours{
			IsOpen:    isOpen,
			NextOpen:  nxtOpen,
			NextClose: nxtClose},
			assetType)
	}
}

func nilMax(a *int64, b *int64) int64 {
	if a == nil {
		return *b
	}
	if b == nil {
		return *a
	}
	if *a > *b {
		return *a
	}
	return *b
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
	var apiResponse PriceFeedApiResponse
	err = json.NewDecoder(response.Body).Decode(&apiResponse)
	if err != nil {
		slog.Error("Error parsing GET request:" + err.Error())
		return
	}
	// check whether id provided is indeed for the symbol we aim to store
	symSource := strings.ToLower(apiResponse.Attributes["generic_symbol"])
	if symSource != strings.ReplaceAll(sym, "-", "") {
		slog.Error("Error: price_feeds GET id is for " + symSource +
			" but symbol " + sym)
		return
	}
	p.setMarketHours(sym, apiResponse.MarketHours, apiResponse.Attributes["asset_type"])
}

func (p *PythHistoryAPI) setMarketHours(ticker string, mh MarketHours, assetType string) error {
	assetType = strings.ToLower(assetType)
	c := *p.RedisClient.Client
	var nxto, nxtc string

	nxto = strconv.FormatInt(mh.NextOpen, 10)
	nxtc = strconv.FormatInt(mh.NextClose, 10)

	c.Do(p.RedisClient.Ctx, c.B().Hset().Key(ticker+":mkt_info").
		FieldValue().FieldValue("is_open", strconv.FormatBool(mh.IsOpen)).
		FieldValue("nxt_open", nxto).
		FieldValue("nxt_close", nxtc).
		FieldValue("asset_type", assetType).Build())
	return nil
}
func (p *PythHistoryAPI) GetMarketInfo(ticker string) (MarketInfo, error) {
	return GetMarketInfo(p.RedisClient.Ctx, p.RedisClient.Client, ticker)
}

func GetMarketInfo(ctx context.Context, client *rueidis.Client, ticker string) (MarketInfo, error) {
	c := *client
	hm, err := c.Do(ctx, c.B().Hgetall().Key(ticker+":mkt_info").Build()).AsStrMap()
	if err != nil {
		return MarketInfo{}, err
	}
	if len(hm) == 0 {
		return MarketInfo{}, errors.New("ticker not found")
	}
	isOpen, _ := strconv.ParseBool(hm["is_open"])
	nxtOpen, _ := strconv.ParseInt(hm["nxt_open"], 10, 64)
	nxtClose, _ := strconv.ParseInt(hm["nxt_close"], 10, 64)
	asset := hm["asset_type"]
	// determine market open/close based on current timestamp and
	// next close ts (can be outdated as long as not outdated for more than
	// closing period)
	now := time.Now().UTC().Unix()
	isOpen = nxtClose == 0 ||
		(nxtOpen < now && now < nxtClose) ||
		(now < nxtClose && nxtClose < nxtOpen)
	var mh = MarketHours{
		IsOpen:    isOpen,
		NextOpen:  nxtOpen,
		NextClose: nxtClose,
	}
	var m = MarketInfo{MarketHours: mh, AssetType: asset}
	return m, nil
}
