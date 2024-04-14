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
	"sync"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	"github.com/redis/rueidis"
)

// buildHistory
func (p *PythHistoryAPI) BuildHistory() {
	var k int
	config := p.SymbolMngr
	pythSyms := make([]utils.SymbolPyth, len(config.PythIdToSym))
	for id, sym := range config.PythIdToSym {
		var symP utils.SymbolPyth
		origin := config.SymToPythOrigin[sym]
		symP.New(origin, id, sym)
		pythSyms[k] = symP
		k++
	}
	slog.Info("-- building history from Pyth candles...")
	p.PythDataToRedisPriceObs(pythSyms)
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
func (p *PythHistoryAPI) RetrieveCandlesFromPyth(sym utils.SymbolPyth, candleRes utils.PythCandleResolution, fromTSSec uint32, toTsSec uint32) (PythHistoryAPIResponse, error) {
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
				return PythHistoryAPIResponse{}, fmt.Errorf("Error making GET request: %v", err)
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
		return PythHistoryAPIResponse{}, fmt.Errorf("Error parsing GET request: %v", err)
	}

	return apiResponse, nil
}

// Query Pyth Candle API and construct artificial price data which
// is stored to Redis
func (p *PythHistoryAPI) PythDataToRedisPriceObs(symbols []utils.SymbolPyth) {
	var wg sync.WaitGroup
	for _, sym := range symbols {
		wg.Add(1)
		go func(sym utils.SymbolPyth) {
			defer wg.Done()

			trial := 0
			var err error
			var o PriceObservations
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
			p.PricesToRedis(sym.Symbol, o)

			slog.Info("Processed history for " + sym.ToString())
		}(sym)
	}
	wg.Wait()
	slog.Info("History of Pyth sources complete")
}

// Extract candles to triangulated candles
// symbols of the form eth-usd
// obsolete:
func (p *PythHistoryAPI) CandlesToTriangulatedCandles(client *utils.RueidisClient, config utils.SymbolManager) {
	var wg sync.WaitGroup

	for sym, path := range config.SymToTriangPath {
		wg.Add(1)
		go func(sym string, path d8x_futures.Triangulation) {
			defer wg.Done()
			o, err := p.ConstructPriceObsForTriang(client, sym, path)
			if err != nil {
				slog.Error("error for triangulation " + sym + ":" + err.Error())
				return
			}
			p.PricesToRedis(sym, o)
			slog.Info("Processed history for " + sym)
		}(sym, path)
	}
	wg.Wait()
	slog.Info("History of Pyth sources complete")
}

// EnableTriangulation constructs candles from existing candles
// that correspond to a price source (e.g. BTC-USD but not BTC-USDC)
// and enables that the candles will be maintained from this point on
func (p *PythHistoryAPI) EnableTriangulation(symT string) bool {
	p.SymbolMngr.SymConstructionMutx.Lock()
	defer p.SymbolMngr.SymConstructionMutx.Unlock()
	config := p.SymbolMngr
	if _, exists := (*config).SymToTriangPath[symT]; exists {
		// already exists
		return true
	}
	path := d8x_futures.Triangulate(symT, config.PriceFeedIds)
	if len(path.Symbol) == 0 {
		slog.Info("Could not triangulate " + symT)
		return false
	}
	config.AddSymbolToTriangTarget(symT, &path)
	config.SymToTriangPath[symT] = path
	p.fetchTriangulatedMktInfo(config)
	o, err := p.ConstructPriceObsForTriang(p.RedisClient, symT, path)
	if err != nil {
		slog.Error("error for triangulation " + symT + ":" + err.Error())
		return false
	}
	p.PricesToRedis(symT, o)
	return true
}

// sym of the form ETH-USD
func (p *PythHistoryAPI) PricesToRedis(sym string, obs PriceObservations) {
	CreateRedisTimeSeries(p.RedisClient, sym)
	var wg sync.WaitGroup
	for k := 0; k < len(obs.P); k++ {
		// store prices in ms
		val := obs.P[k]
		t := int64(obs.T[k]) * 1000
		wg.Add(1)
		go func(sym string, t int64, val float64) {
			defer wg.Done()
			AddPriceObs(p.RedisClient, sym, t, val)
		}(sym, t, val)
	}
	// set the symbol as available
	c := *p.RedisClient.Client
	c.Do(context.Background(), c.B().Sadd().Key(utils.AVAIL_TICKER_SET).Member(sym).Build())
	wg.Wait()
}

// Query specific candle resolutions and time ranges from the Pyth-API
// and construct artificial data
func (p *PythHistoryAPI) ConstructPriceObsFromPythCandles(sym utils.SymbolPyth) (PriceObservations, error) {
	var candleRes utils.PythCandleResolution
	candleRes.New(1, utils.MinuteCandle)
	currentTimeSec := uint32(time.Now().UTC().Unix())
	twoDayResolutionMinute, err := p.RetrieveCandlesFromPyth(sym, candleRes, currentTimeSec-86400*2, currentTimeSec)
	if err != nil {
		return PriceObservations{}, err
	}

	candleRes.New(60, utils.MinuteCandle)
	oneMonthResolution1h, err := p.RetrieveCandlesFromPyth(sym, candleRes, currentTimeSec-86400*30, currentTimeSec)
	if err != nil {
		return PriceObservations{}, err
	}
	candleRes.New(1, utils.DayCandle)
	// jan1 2022: 1640995200
	allTimeResolution1D, err := p.RetrieveCandlesFromPyth(sym, candleRes, 1640995200, currentTimeSec)
	if err != nil {
		return PriceObservations{}, err
	}
	var candles = []PythHistoryAPIResponse{twoDayResolutionMinute, oneMonthResolution1h, allTimeResolution1D}
	// concatenate candles into price observations
	var obs PriceObservations
	obs, err = PythCandlesToPriceObs(candles)
	if err != nil {
		return PriceObservations{}, err
	}
	return obs, nil
}

// Construct price observations for triangulated currencies
// symT is of the form btc-usd, path a Triangulation type
func (p *PythHistoryAPI) ConstructPriceObsForTriang(client *utils.RueidisClient, symT string, path d8x_futures.Triangulation) (PriceObservations, error) {
	currentTimeSec := uint32(time.Now().UTC().Unix())
	// find starting time
	var timeStart, timeEnd int64 = 0, int64(currentTimeSec) * 1000
	for k := 1; k < len(path.Symbol); k++ {
		sym := path.Symbol[k]
		info, err := (*client.Client).Do(client.Ctx, (*client.Client).B().
			TsInfo().Key(sym).Build()).AsMap()

		if err != nil {
			// key does not exist
			return PriceObservations{}, errors.New(err.Error() + " symbol:" + sym)
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
	var candles = [][]OhlcData{oneDayResolutionMinute, twoDayResolution5Minute, oneMonthResolution1h, allTimeResolution1D}
	var obs PriceObservations
	obs, err = OhlcCandlesToPriceObs(candles)
	if err != nil {
		return PriceObservations{}, err
	}
	return obs, nil
}

func (p *PythHistoryAPI) triangulateCandles(client *utils.RueidisClient, path d8x_futures.Triangulation, fromTsMs int64, toTsMs int64, resolSec uint32) ([]OhlcData, error) {
	var ohlcPath []*[]OhlcData
	var maxStart int64 = 0
	for k := 0; k < len(path.Symbol); k++ {
		sym := path.Symbol[k]
		ohlc, err := Ohlc(client, sym, fromTsMs, toTsMs, resolSec)
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
	if (*ohlcPath[0])[0].TsMs != (*ohlcPath[1])[0].TsMs {
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

// SubscribeTickerRequest the to redis pub/sub utils.TICKER_REQUEST
// makes triangulation available if possible
func (p *PythHistoryAPI) SubscribeTickerRequest() error {
	client := *p.RedisClient.Client
	err := client.Receive(context.Background(), client.B().Subscribe().Channel(utils.TICKER_REQUEST).Build(),
		func(msg rueidis.PubSubMessage) {
			slog.Info("Enabling Triangulation: " + msg.Message)
			p.EnableTriangulation(msg.Message)
			fmt.Printf("\n")
		})
	return err
}

func (p *PythHistoryAPI) OnPriceUpdate(pxResp utils.PriceUpdateResponse, sym string, lastPx map[string]float64) {
	if sym == "" {
		return
	}
	px := CalcPrice(pxResp)
	// no check whether price is identical, because we want the candles to potentially match open/close
	lastPx[sym] = px
	AddPriceObs(p.RedisClient, sym, pxResp.PriceFeed.Price.PublishTime*1000, px)

	slog.Info("Received price update: " + sym + " price=" + fmt.Sprint(px) + fmt.Sprint(pxResp.PriceFeed))

	pubMsg := sym
	// triangulations
	targetSymbols := p.SymbolMngr.SymToDependentTriang[sym]
	for _, tsym := range targetSymbols {
		// if market closed for any of the items in the triangulation,
		if p.IsTriangulatedMarketClosed(tsym, p.SymbolMngr.SymToTriangPath[tsym].Symbol) {
			slog.Info("-- triangulation price update: " + tsym + " - market closed")
			continue
		}
		// we should not publish an update
		pxTriang := utils.TriangulatedPx(p.SymbolMngr.SymToTriangPath[tsym], lastPx)
		if pxTriang == 0 {
			slog.Info("-- triangulation currently not possible: " + tsym)
			continue
		}
		lastPx[tsym] = pxTriang
		pubMsg += ";" + tsym
		AddPriceObs(p.RedisClient, tsym, pxResp.PriceFeed.Price.PublishTime*1000, pxTriang)
		slog.Info("-- triangulation price update: " + tsym + " price=" + fmt.Sprint(pxTriang))
	}
	// publish updates to listeners
	client := *p.RedisClient.Client
	err := client.Do(context.Background(), client.B().Publish().Channel(utils.PRICE_UPDATE_MSG).Message(pubMsg).Build()).Error()
	if err != nil {
		slog.Error("Redis Pub" + err.Error())
	}
}

// check whether any of the symbols in the triangulation has a closed market
func (p *PythHistoryAPI) IsTriangulatedMarketClosed(tsym string, symbols []string) bool {
	client := p.RedisClient
	info, err := GetMarketInfo(context.Background(), client.Client, tsym)
	if err != nil {
		slog.Info("Market-closed info " + tsym + ":" + err.Error())
		return true
	}
	if !info.MarketHours.IsOpen {
		return true
	}
	for _, sym := range symbols {
		info, err := GetMarketInfo(context.Background(), client.Client, tsym)
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

// calculate floating point price from 'price' and 'expo'
func CalcPrice(pxResp utils.PriceUpdateResponse) float64 {
	x, err := strconv.Atoi(pxResp.PriceFeed.Price.Price)
	if err != nil {
		slog.Error("onPriceUpdate error" + err.Error())
		return 0
	}
	pw := float64(pxResp.PriceFeed.Price.Expo)
	px := float64(x) * math.Pow(10, pw)
	return px
}
