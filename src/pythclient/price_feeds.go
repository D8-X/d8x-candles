package pythclient

import (
	"context"
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"time"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
)

type PriceFeedApiResponse struct {
	ID          string            `json:"id"`
	MarketHours utils.MarketHours `json:"market_hours"`
	Attributes  map[string]string `json:"attributes"`
}

// Runs FetchMktHours and schedules next runs
func (p *PythClientApp) ScheduleMktInfoUpdate(updtInterval time.Duration) {
	tickerUpdate := time.NewTicker(updtInterval)
	for {
		<-tickerUpdate.C

		slog.Info("Updating market info...")
		p.StreamMngr.asymRWMu.RLock()
		syms := make([]string, 0, len(p.StreamMngr.activeSyms))
		for s := range p.StreamMngr.activeSyms {
			syms = append(syms, s)
		}
		p.StreamMngr.asymRWMu.RUnlock()
		p.FetchMktInfo(syms)
		fmt.Println("Market info updated.")

	}
}

// FetchMktInfo goes through all symbols in the config, including triangulated ones,
// and fetches market hours (next open, next close). Stores in
// Redis
func (p *PythClientApp) FetchMktInfo(symbols []string) {
	// process base price feeds (no triangulation)
	config := p.SymbolMngr
	for _, sym := range symbols {
		id := ""
		for id0, sym0 := range config.PythIdToSym {
			if sym0 == sym {
				id = id0
				break
			}
		}
		if id == "" {
			continue
		}
		origin := config.SymToPythOrigin[sym]
		asset := strings.ToLower(strings.Split(origin, ".")[0])
		if asset == "crypto" {
			// crypto markets are always open, huray
			p.setMarketHours(sym, utils.MarketHours{IsOpen: true, NextOpen: 0, NextClose: 0}, d8xUtils.ACLASS_CRYPTO)
			continue
		}
		slog.Info("Fetching market info for " + sym)
		err := p.QueryPriceFeedInfo(sym, origin, id)
		if err != nil {
			slog.Error("QueryPriceFeedInfo", "error", err)
		}
	}
	// construct info for all triangulated price feeds, e.g. chf-usdc
	p.fetchTriangulatedMktInfo()
}

// set market hours in redis for all triangulations
func (p *PythClientApp) fetchTriangulatedMktInfo() {
	p.StreamMngr.symToTriangPathRWMu.RLock()
	defer p.StreamMngr.symToTriangPathRWMu.RUnlock()
outerLoop:
	for symT, path := range p.StreamMngr.SymToTriangPath {
		isOpen := true
		var nxtOpen, nxtClose int64 = 0, math.MaxInt64
		assetType := d8xUtils.ACLASS_CRYPTO
		for k := 0; k < len(path.Symbol); k++ {
			m, err := p.GetMarketInfo(path.Symbol[k])
			if err != nil {
				p.FetchMktInfo([]string{path.Symbol[k]})
				slog.Error("Error triangulated feeds info " + symT + " at " + path.Symbol[k])
				continue outerLoop
			}
			if m.AssetType != d8xUtils.ACLASS_CRYPTO {
				// dominant asset type for triangulations is
				// the non-crypto asset
				assetType = m.AssetType
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
		if assetType == d8xUtils.ACLASS_CRYPTO {
			nxtClose = 0
		}

		p.setMarketHours(symT, utils.MarketHours{
			IsOpen:    isOpen,
			NextOpen:  nxtOpen,
			NextClose: nxtClose},
			assetType)
	}
}

func (p *PythClientApp) QueryPriceFeedInfo(sym, origin string, id string) error {
	// example: https://benchmarks.pyth.network/v1/price_feeds/0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace
	const endpoint = "/v1/price_feeds/"
	// we need the mainnet id
	url := strings.TrimSuffix(p.BaseUrl, "/") + endpoint + id
	// Send a GET request
	var response *http.Response
	var err error
	for {
		if p.TokenBucket.Take() {
			response, err = http.Get(url)
			if err != nil {
				return fmt.Errorf("GET request: %v", err.Error())
			}
			break
		}
		slog.Info("too many requests [PriceFeed], slowing down for " + sym)
		time.Sleep(time.Duration(rand.Intn(25)) * time.Millisecond)
	}
	defer response.Body.Close()
	// Check response status code
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code[PriceFeed]: %d for url %s", response.StatusCode, url)
	}
	// Read the response body
	var apiResponse PriceFeedApiResponse
	err = json.NewDecoder(response.Body).Decode(&apiResponse)
	if err != nil {
		return fmt.Errorf("parsing GET request: %v", err.Error())
	}
	// check whether id provided is indeed for the symbol we aim to store
	symSource := strings.ToUpper(apiResponse.Attributes["generic_symbol"])
	symSource2 := strings.ToUpper(apiResponse.Attributes["symbol"])
	if symSource != strings.ReplaceAll(strings.ToUpper(sym), "-", "") &&
		!strings.EqualFold(origin, symSource2) {
		return fmt.Errorf("QueryPriceFeedInfo: price_feeds GET id is for %s/%s not in line with %s", symSource, symSource2, sym)
	}
	asset := d8xUtils.OriginToAssetClass(apiResponse.Attributes["symbol"]) //"Crypto.ETH/USD"
	return p.setMarketHours(sym, apiResponse.MarketHours, asset)
}

func (p *PythClientApp) setMarketHours(ticker string, mh utils.MarketHours, assetType d8xUtils.AssetClass) error {
	return utils.RedisSetMarketHours(p.RedisClient, ticker, mh, assetType)
}

func (p *PythClientApp) GetMarketInfo(ticker string) (utils.MarketInfo, error) {
	return utils.RedisGetMarketInfo(context.Background(), p.RedisClient, ticker)
}
