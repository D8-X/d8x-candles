package pythclient

import (
	"context"
	"d8x-candles/src/utils"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
)

// buildHistory
func (p *PythClientApp) BuildHistory(symbols []string) error {
	config := p.SymbolMngr
	pythSyms := make([]utils.SymbolPyth, 0, len(symbols))
	for _, sym := range symbols {
		id := ""
		for id0, sym0 := range config.PythIdToSym {
			if sym0 == sym {
				id = id0
				break
			}
		}
		if id == "" {
			return fmt.Errorf("symbol %s not available in config", sym)
		}
		var symP utils.SymbolPyth
		origin := config.SymToPythOrigin[sym]
		symP.New(origin, id, sym)
		pythSyms = append(pythSyms, symP)
	}
	slog.Info("-- building history from Pyth candles...")
	p.PythDataToRedisPriceObs(pythSyms)
	return nil
}

/*
Available:
1 -> 1 minute (works for 24h)
2, 5, 15, 30, 60, 120, 240, 360, 720 -> min
1D, 1W, 1M

Process
1 Minute -> 1 day. OK
5 minutes -> 2 days.  OK
1h -> 1 month OK
1 day -> all history
*/
func (p *PythClientApp) RetrieveCandlesFromPyth(sym utils.SymbolPyth, candleRes utils.PythCandleResolution, fromTSSec uint32, toTsSec uint32) (PythHistoryAPIResponse, error) {
	const endpoint = "/v1/shims/tradingview/history"
	query := "?symbol=" + sym.ToString() + "&resolution=" + candleRes.ToPythString() + "&from=" + strconv.Itoa(int(fromTSSec)) + "&to=" + strconv.Itoa(int(toTsSec))

	url := strings.TrimSuffix(p.BaseUrl, "/") + endpoint + query
	// Send a GET request
	var response *http.Response
	var err error
	for {
		if p.TokenBucket.Take() {
			response, err = http.Get(url)
			if err != nil {
				return PythHistoryAPIResponse{}, fmt.Errorf("error making GET request: %v", err)
			}
			break
		}
		slog.Info("too many requests, slowing down for " + sym.Symbol)
		time.Sleep(time.Duration(50+rand.Intn(250)) * time.Millisecond)
	}

	defer response.Body.Close()

	// Check response status code
	if response.StatusCode != http.StatusOK {
		return PythHistoryAPIResponse{}, fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	// Read the response body
	var apiResponse PythHistoryAPIResponse
	err = json.NewDecoder(response.Body).Decode(&apiResponse)
	if err != nil {
		return PythHistoryAPIResponse{}, fmt.Errorf("error parsing GET request: %v", err)
	}
	if apiResponse.S == "error" {
		return PythHistoryAPIResponse{}, fmt.Errorf("error in benchmarks GET request: %s", apiResponse.ErrMsg)
	}

	return apiResponse, nil
}

// Query Pyth Candle API and construct artificial price data which
// is stored to Redis
func (p *PythClientApp) PythDataToRedisPriceObs(symbols []utils.SymbolPyth) {
	var wg sync.WaitGroup
	for _, sym := range symbols {
		wg.Add(1)
		go func(sym utils.SymbolPyth) {
			defer wg.Done()

			trial := 0
			var err error
			var o utils.PriceObservations
			for {
				o, err = p.ConstructPriceObsFromPythCandles(sym)
				if err == nil {
					break
				}
				if trial < 10 {
					slog.Info("pyth query failed for " + sym.ToString() + ":" + err.Error() + ". Retrying.")
					// https://docs.pyth.network/benchmarks/rate-limits
					time.Sleep(time.Duration(rand.Intn(60_000)+60_000) * time.Millisecond)
				} else {
					slog.Error("pyth query failed for " + sym.ToString() + ":" + err.Error())
					return
				}
				trial++
			}
			err = utils.PricesToRedis(p.RedisClient, sym.Symbol, d8xUtils.PXTYPE_PYTH, o)
			if err != nil {
				slog.Error("pyth PricesToRedis failed for " + sym.ToString() + ":" + err.Error())
				return
			}
			slog.Info("Processed history for " + sym.ToString())
		}(sym)
	}
	wg.Wait()
	slog.Info("History of Pyth sources complete")
}

// Extract candles to triangulated candles
// symbols of the form eth-usd
// obsolete:
func (p *PythClientApp) CandlesToTriangulatedCandles(client *rueidis.Client, config utils.SymbolManager) {
	var wg sync.WaitGroup

	p.StreamMngr.symToTriangPathRWMu.RLock()
	for sym, path := range p.StreamMngr.SymToTriangPath {
		wg.Add(1)
		// copy to avoid concurrency issues
		pathCp := d8x_futures.Triangulation{
			IsInverse: make([]bool, len(path.IsInverse)),
			Symbol:    make([]string, len(path.Symbol)),
		}
		copy(pathCp.IsInverse, path.IsInverse)
		copy(pathCp.Symbol, path.Symbol)
		go func(sym string, path d8x_futures.Triangulation) {
			defer wg.Done()
			o, err := p.ConstructPriceObsForTriang(client, sym, path)
			if err != nil {
				slog.Error("triangulation " + sym + ":" + err.Error())
				return
			}
			err = utils.PricesToRedis(client, sym, d8xUtils.PXTYPE_PYTH, o)
			if err != nil {
				slog.Error("triangulation " + sym + ":" + err.Error())
				return
			}
			slog.Info("Processed history for " + sym)
		}(sym, pathCp)
	}
	p.StreamMngr.symToTriangPathRWMu.RUnlock()
	wg.Wait()
	slog.Info("History of Pyth sources complete")
}

// Query specific candle resolutions and time ranges from the Pyth-API
// and construct artificial data
func (p *PythClientApp) ConstructPriceObsFromPythCandles(sym utils.SymbolPyth) (utils.PriceObservations, error) {
	var candleRes utils.PythCandleResolution
	candleRes.New(1, utils.MinuteCandle)
	currentTimeSec := uint32(time.Now().UTC().Unix())
	twoDayResolutionMinute, err := p.RetrieveCandlesFromPyth(sym, candleRes, currentTimeSec-86400*2, currentTimeSec)
	if err != nil {
		return utils.PriceObservations{}, err
	}

	candleRes.New(60, utils.MinuteCandle)
	oneMonthResolution1h, err := p.RetrieveCandlesFromPyth(sym, candleRes, currentTimeSec-86400*30, currentTimeSec)
	if err != nil {
		return utils.PriceObservations{}, err
	}
	candleRes.New(1, utils.DayCandle)
	// only one year allowed
	dateTs := uint32(time.Now().Unix() - 3.15e7)
	allTimeResolution1D, err := p.RetrieveCandlesFromPyth(sym, candleRes, dateTs, currentTimeSec)
	if err != nil {
		return utils.PriceObservations{}, err
	}
	var candles = []PythHistoryAPIResponse{twoDayResolutionMinute, oneMonthResolution1h, allTimeResolution1D}
	// concatenate candles into price observations
	var obs utils.PriceObservations
	obs, err = PythCandlesToPriceObs(candles)
	if err != nil {
		return utils.PriceObservations{}, err
	}
	return obs, nil
}

// Construct price observations for triangulated currencies
// symT is of the form btc-usd, path a Triangulation type
func (p *PythClientApp) ConstructPriceObsForTriang(client *rueidis.Client, symT string, path d8x_futures.Triangulation) (utils.PriceObservations, error) {
	currentTimeSec := uint32(time.Now().UTC().Unix())
	// find starting time
	var timeStart, timeEnd int64 = 0, int64(currentTimeSec) * 1000
	for k := 0; k < len(path.Symbol); k++ {
		key := d8xUtils.PXTYPE_PYTH.String() + ":" + path.Symbol[k]
		info, err := (*client).Do(context.Background(), (*client).B().
			TsInfo().Key(key).Build()).AsMap()

		if err != nil {
			// key does not exist
			return utils.PriceObservations{}, errors.New(err.Error() + " symbol:" + key)
		}
		first := info["firstTimestamp"]
		last := info["lastTimestamp"]
		fromTsMs, _ := (&first).AsInt64()
		toTsMs, _ := (&last).AsInt64()
		if fromTsMs > timeStart {
			timeStart = fromTsMs
		}
		if timeEnd > toTsMs {
			timeEnd = toTsMs
		}
	}
	// candles

	oneDayResolutionMinute, err := p.triangulateCandles(client, path, timeEnd-86400000, timeEnd, 60)
	if err != nil {
		slog.Error("1min resolution triangulation error " + err.Error())
	}
	twoDayResolution5Minute, err := p.triangulateCandles(client, path, timeEnd-86400000*2, timeEnd-86400000, 5*60)
	if err != nil {
		slog.Error("5min resolution triangulation error " + err.Error())
	}
	oneMonthResolution1h, err := p.triangulateCandles(client, path, timeEnd-86400000*30, timeEnd-86400000*2, 60*60)
	if err != nil {
		slog.Error("1h resolution triangulation error " + err.Error())
	}
	allTimeResolution1D, err := p.triangulateCandles(client, path, timeStart, timeEnd-86400000*30, 24*60*60)
	if err != nil {
		slog.Error("1day resolution triangulation error " + err.Error())
	}
	var candles = [][]utils.OhlcData{oneDayResolutionMinute, twoDayResolution5Minute, oneMonthResolution1h, allTimeResolution1D}
	var obs utils.PriceObservations
	obs, err = utils.OhlcCandlesToPriceObs(candles, symT)
	if err != nil {
		return utils.PriceObservations{}, err
	}
	return obs, nil
}

func (p *PythClientApp) triangulateCandles(client *rueidis.Client, path d8x_futures.Triangulation, fromTsMs int64, toTsMs int64, resolSec uint32) ([]utils.OhlcData, error) {
	var ohlcPath []*[]utils.OhlcData
	var maxStart int64 = 0
	for k := 0; k < len(path.Symbol); k++ {
		sym := path.Symbol[k]
		ohlc, err := utils.OhlcFromRedis(client, sym, d8xUtils.PXTYPE_PYTH, fromTsMs, toTsMs, resolSec)
		if err != nil || len(ohlc) == 0 {
			return nil, errors.New("ohlc not available for " + sym)
		}
		if ohlc[0].TsMs > maxStart {
			maxStart = ohlc[0].TsMs
		}
		ohlcPath = append(ohlcPath, &ohlc)
	}
	// if no observation falls in one bucket, redis will omit the bucket,
	// hence we need to align the candles starting times
	for k := 0; k < len(ohlcPath); k++ {
		ohlcCurr := *ohlcPath[k]
		var tau int = 0
		for t := 0; t < len(ohlcCurr); t++ {
			if ohlcCurr[t].TsMs == maxStart {
				tau = t
				break
			}
		}
		if tau > 0 {
			reducedSlice := ohlcCurr[tau:]
			ohlcPath[k] = &reducedSlice
		}

	}
	if len(ohlcPath) > 1 && (*ohlcPath[0])[0].TsMs != (*ohlcPath[1])[0].TsMs {
		panic("triangulateCandles mismatch")
	}
	T := len(*ohlcPath[0])
	ohlcRes := *ohlcPath[0]
	if path.IsInverse[0] {
		for t := 0; t < T; t++ {
			ohlcRes[t].O = 1 / ohlcRes[t].O
			ohlcRes[t].C = 1 / ohlcRes[t].C
			h := ohlcRes[t].H
			ohlcRes[t].H = 1 / ohlcRes[t].L
			ohlcRes[t].L = 1 / h
		}
	}
	for j := 1; j < len(path.Symbol); j++ {
		if !path.IsInverse[j] {
			for t := 0; t < T; t++ {
				ohlcRes[t].O = ohlcRes[t].O * (*ohlcPath[j])[t].O
				ohlcRes[t].C = ohlcRes[t].C * (*ohlcPath[j])[t].C
				ohlcRes[t].H = ohlcRes[t].H * (*ohlcPath[j])[t].H
				ohlcRes[t].L = ohlcRes[t].L * (*ohlcPath[j])[t].L
			}
		} else { // inverse
			for t := 0; t < T; t++ {
				ohlcRes[t].O = ohlcRes[t].O / (*ohlcPath[j])[t].O
				ohlcRes[t].C = ohlcRes[t].C / (*ohlcPath[j])[t].C
				ohlcRes[t].H = ohlcRes[t].H / (*ohlcPath[j])[t].L
				ohlcRes[t].L = ohlcRes[t].L / (*ohlcPath[j])[t].H
			}
		}
	}
	return ohlcRes, nil
}

func (p *PythClientApp) OnPriceUpdate(pxData utils.PriceData, id string) {
	sym := p.SymbolMngr.PythIdToSym[id]
	if sym == "" {
		return
	}
	px := pxData.CalcPrice()
	// no check whether price is identical, because we want the candles to potentially match open/close
	p.StreamMngr.lastPxRWMu.Lock()
	p.StreamMngr.lastPx[sym] = px
	p.StreamMngr.lastPxRWMu.Unlock()

	utils.RedisAddPriceObs(p.RedisClient, d8xUtils.PXTYPE_PYTH, sym, px, pxData.PublishTime*1000)

	p.MsgCount["px"] = (p.MsgCount["px"] + 1) % 500
	if p.MsgCount["px"] == 0 {
		slog.Info(fmt.Sprintf("Received 500 price updates since last report. Now %s, price=%.4f", sym, px))
	}

	pubMsg := d8xUtils.PXTYPE_PYTH.String() + ":" + sym

	// triangulations
	p.StreamMngr.s2DTRWMu.RLock()
	targetSymbols := p.StreamMngr.symToDependentTriang[sym]
	// copy concurrent data into new slice
	symbols := make([]string, len(targetSymbols))
	copy(symbols, targetSymbols)
	p.StreamMngr.s2DTRWMu.RUnlock()

	p.StreamMngr.symToTriangPathRWMu.RLock()
	defer p.StreamMngr.symToTriangPathRWMu.RUnlock()
	for _, tsym := range symbols {
		// if market closed for any of the items in the triangulation,

		if p.IsTriangulatedMarketClosed(tsym, p.StreamMngr.SymToTriangPath[tsym].Symbol) {
			p.MsgCount["tclosed"] = (p.MsgCount["tclosed"] + 1) % 500
			if p.MsgCount["tclosed"] == 0 {
				slog.Info("-- triangulation price update: " + tsym + " - market closed")
			}
			continue
		}
		// we should not publish an update
		p.StreamMngr.lastPxRWMu.RLock()
		pxTriang := utils.TriangulatedPx(p.StreamMngr.SymToTriangPath[tsym], p.StreamMngr.lastPx)
		p.StreamMngr.lastPxRWMu.RUnlock()

		if pxTriang == 0 {
			slog.Info("-- triangulation currently not possible: " + tsym)
			continue
		}
		p.StreamMngr.lastPxRWMu.Lock()
		p.StreamMngr.lastPx[tsym] = pxTriang
		p.StreamMngr.lastPxRWMu.Unlock()

		pubMsg += ";" + d8xUtils.PXTYPE_PYTH.String() + ":" + tsym
		utils.RedisAddPriceObs(p.RedisClient, d8xUtils.PXTYPE_PYTH, tsym, pxTriang, pxData.PublishTime*1000)
		p.MsgCount["t"] = (p.MsgCount["t"] + 1) % 500
		if p.MsgCount["t"] == 0 {
			slog.Info("-- 500 triangulation price updates, now: " + tsym + " price=" + fmt.Sprint(pxTriang))
		}
	}

	// publish updates to listeners
	err := utils.RedisPublishIdxPriceChange(p.RedisClient, pubMsg)
	if err != nil {
		slog.Error("Redis Pub" + err.Error())
	}
}

// check whether any of the symbols in the triangulation has a closed market
func (p *PythClientApp) IsTriangulatedMarketClosed(tsym string, symbols []string) bool {
	client := p.RedisClient
	info, err := utils.RedisGetMarketInfo(context.Background(), client, tsym)
	if err != nil {
		slog.Info("Market-closed info " + tsym + ":" + err.Error())
		return true
	}
	if !info.MarketHours.IsOpen {
		return true
	}
	for _, sym := range symbols {
		info, err := utils.RedisGetMarketInfo(context.Background(), client, tsym)
		if err != nil {
			slog.Error("Error market-closed info " + tsym + "(" + sym + "):" + err.Error())
			return true
		}
		if !info.MarketHours.IsOpen {
			return true
		}
	}
	return false
}
