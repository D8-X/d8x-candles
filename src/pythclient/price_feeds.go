package pythclient

import (
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"time"
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
			p.setMarketHours(sym, utils.MarketHours{IsOpen: true, NextOpen: 0, NextClose: 0}, "crypto")
			continue
		}
		slog.Info("Fetching market info for " + sym)
		p.QueryPriceFeedInfo(sym, id)
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
		var assetType string = "crypto"
		for k := 0; k < len(path.Symbol); k++ {
			m, err := p.GetMarketInfo(path.Symbol[k])
			if err != nil {
				p.FetchMktInfo([]string{path.Symbol[k]})
				slog.Error("Error triangulated feeds info " + symT + " at " + path.Symbol[k])
				continue outerLoop
			}
			if m.AssetType != "crypto" {
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
		if assetType == "crypto" {
			nxtClose = 0
		}

		p.setMarketHours(symT, utils.MarketHours{
			IsOpen:    isOpen,
			NextOpen:  nxtOpen,
			NextClose: nxtClose},
			assetType)
	}
}

func (p *PythClientApp) QueryPriceFeedInfo(sym string, id string) {
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
		slog.Error("unexpected status code[PriceFeed]: " + fmt.Sprintf("%d for url %s", response.StatusCode, url))
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
	symSource := strings.ToUpper(apiResponse.Attributes["generic_symbol"])
	if symSource != strings.ReplaceAll(sym, "-", "") {
		slog.Error("Error: price_feeds GET id is for " + symSource +
			" but symbol " + sym)
		return
	}
	p.setMarketHours(sym, apiResponse.MarketHours, apiResponse.Attributes["asset_type"])
}

func (p *PythClientApp) setMarketHours(ticker string, mh utils.MarketHours, assetType string) error {
	assetType = strings.ToLower(assetType)
	return utils.SetMarketHours(p.RedisClient.Client, ticker, mh, assetType)
}

func (p *PythClientApp) GetMarketInfo(ticker string) (utils.MarketInfo, error) {
	return utils.GetMarketInfo(p.RedisClient.Ctx, p.RedisClient.Client, ticker)
}
