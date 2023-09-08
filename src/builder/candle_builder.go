package builder

import (
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
)

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
		time.Sleep(time.Duration(rand.Intn(25)) * time.Millisecond)
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
					time.Sleep(time.Duration(rand.Intn(250)+50) * time.Millisecond)
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

// symbols of the form eth-usd
func (p *PythHistoryAPI) CandlesToTriangulatedCandles(client *utils.RueidisClient, config utils.PriceConfig) {
	var wg sync.WaitGroup

	for sym, path := range config.SymToTriangPath {
		wg.Add(1)
		go func(sym string, path []string) {
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

// sym of the form eth-usd
func (p *PythHistoryAPI) PricesToRedis(sym string, obs PriceObservations) {
	CreateTimeSeries(p.RedisClient, sym)
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
// symT is of the form btc-usd, path the result of config.SymToTriangPath[symT]
func (p *PythHistoryAPI) ConstructPriceObsForTriang(client *utils.RueidisClient, symT string, path []string) (PriceObservations, error) {
	currentTimeSec := uint32(time.Now().UTC().Unix())
	// find starting time
	var timeStart, timeEnd int64 = 0, int64(currentTimeSec) * 1000
	for k := 1; k < len(path); k = k + 2 {
		sym := path[k]
		info, err := (*client.Client).Do(client.Ctx, (*client.Client).B().
			TsInfo().Key(sym).Build()).AsMap()

		if err != nil {
			// key does not exist
			return PriceObservations{}, errors.New("symbol not in REDIS" + sym)
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

func (p *PythHistoryAPI) triangulateCandles(client *utils.RueidisClient, path []string, fromTsMs int64, toTsMs int64, resolSec uint32) ([]OhlcData, error) {
	var ohlcPath []*[]OhlcData
	for k := 1; k < len(path); k = k + 2 {
		ohlc, err := Ohlc(client, path[k], fromTsMs, toTsMs, resolSec)
		if err != nil {
			return nil, errors.New("ohlc not available for " + path[k])
		}
		ohlcPath = append(ohlcPath, &ohlc)
	}
	T := len(*ohlcPath[0])
	ohlcRes := *ohlcPath[0]
	if path[0] == "/" {
		for t := 0; t < T; t++ {
			ohlcRes[t].O = 1 / ohlcRes[t].O
			ohlcRes[t].C = 1 / ohlcRes[t].C
			ohlcRes[t].H = 1 / ohlcRes[t].L
			ohlcRes[t].L = 1 / ohlcRes[t].H
		}
	}
	for k := 3; k < len(path); k = k + 2 {
		j := k / 2

		if path[k-1] == "*" {
			for t := 0; t < T; t++ {
				ohlcRes[t].O = ohlcRes[t].O * (*ohlcPath[j])[t].O
				ohlcRes[t].C = ohlcRes[t].C * (*ohlcPath[j])[t].C
				ohlcRes[t].H = ohlcRes[t].H * (*ohlcPath[j])[t].H
				ohlcRes[t].L = ohlcRes[t].L * (*ohlcPath[j])[t].L
			}
		} else if path[k-1] == "/" {
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
