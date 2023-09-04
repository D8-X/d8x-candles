package builder

import (
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"log/slog"
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
	response, err := http.Get(url)
	if err != nil {
		return PythHistoryAPIResponse{}, fmt.Errorf("Error making GET request: %v", err)
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
			o, err := p.ConstructPriceObsFromPythCandles(sym)
			if err != nil {
				slog.Error("error for " + sym.ToString() + ":" + err.Error())
				return
			}
			p.PricesToRedis(sym, o)
			slog.Info("Processed history for " + sym.ToString())
		}(sym)
	}
	wg.Wait()
	slog.Info("History of Pyth sources complete")
}

func (p *PythHistoryAPI) PricesToRedis(sym utils.SymbolPyth, obs PriceObservations) {
	CreateTimeSeries(p.RedisClient, sym)
	for k := 0; k < len(obs.P); k++ {
		AddPriceObs(p.RedisClient, sym, int64(obs.T[k]), obs.P[k])
	}
}

// Query specific candle resolutions and time ranges from the Pyth-API
// and construct artificial data
func (p *PythHistoryAPI) ConstructPriceObsFromPythCandles(sym utils.SymbolPyth) (PriceObservations, error) {
	var candleRes utils.PythCandleResolution
	candleRes.New(1, utils.MinuteCandle)
	currentTime := uint32(time.Now().Unix())
	oneDayResolutionMinute, err := p.RetrieveCandlesFromPyth(sym, candleRes, currentTime-86400, currentTime)
	if err != nil {
		return PriceObservations{}, err
	}
	candleRes.New(5, utils.MinuteCandle)
	twoDayResolution5Minute, err := p.RetrieveCandlesFromPyth(sym, candleRes, currentTime-86400*2, currentTime)
	if err != nil {
		return PriceObservations{}, err
	}
	candleRes.New(60, utils.MinuteCandle)
	oneMonthResolution1h, err := p.RetrieveCandlesFromPyth(sym, candleRes, currentTime-86400*2, currentTime)
	if err != nil {
		return PriceObservations{}, err
	}
	candleRes.New(1, utils.DayCandle)
	// jan1 2022: 1640995200
	allTimeResolution1D, err := p.RetrieveCandlesFromPyth(sym, candleRes, 1640995200, currentTime)
	if err != nil {
		return PriceObservations{}, err
	}
	var candles = []PythHistoryAPIResponse{oneDayResolutionMinute, twoDayResolution5Minute, oneMonthResolution1h, allTimeResolution1D}
	// concatenate candles into price observations
	var obs PriceObservations
	obs, err = CandlesToPriceObs(candles)

	return obs, nil
}
