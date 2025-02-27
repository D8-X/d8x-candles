package uniutils

import (
	"d8x-candles/src/utils"
	"fmt"
	"log/slog"
	"strings"

	"github.com/redis/rueidis"
)

// combineWithPyth combine uni prices with pyth, start at startMs
// The variable pyth maps a symbol of the form USDC-USD to a candle
func (fltr *Filter) combineWithPyth(pyth map[string]*utils.PythHistoryAPIResponse, client *rueidis.Client, startMs int64) error {
	for _, idx := range fltr.Indices {
		if idx.Symbol == idx.FromPools {
			// index is obtained from pools entirely,
			// no multiplication with pyth price needed
			if idx.ContractSize != 1 {
				return fmt.Errorf("symbol %s inconsistent with contract size %f", idx.Symbol, idx.ContractSize)
			}
			continue
		}
		// split off multiplier (e.g. PEPE-USD.1000 -> PEPE-USD)
		idxSym := strings.Split(idx.Symbol, ".")[0]

		if idx.FromPyth == "" && idxSym != idx.FromPools {
			// configuration error
			return fmt.Errorf("config error: idx symbol %s not equal to fromPools %s but no fromPyth", idxSym, idx.FromPools)
		}
		// get all observations of the prices that we have from the pool-requests
		obs, err := utils.RedisGetRangeFrom(
			client,
			fltr.UniType,
			idx.FromPools,
			startMs,
		)
		if err != nil {
			return fmt.Errorf("unable to get range prices for %s: %v", idx.FromPools, err)
		}
		for _, o := range obs {
			px := float64(1)
			if idx.FromPyth != "" {
				px, err = findOpen(o.TsMs, pyth[idx.FromPyth])
				if err != nil {
					slog.Info("did not find price from candles", "symbol", idx.FromPyth, "error", err)
					continue
				}
			}
			price := px * o.Px * idx.ContractSize
			err = utils.RedisAddPriceObs(client, fltr.UniType, idx.Symbol, price, o.TsMs)
			if err != nil {
				slog.Error("unable to add index price to redis", "symbol", idxSym, "error", err)
			}
		}
	}
	return nil
}

func findOpen(tsMs int64, candles *utils.PythHistoryAPIResponse) (float64, error) {
	tsSec := uint32(tsMs / 1000)
	if candles.T[0] > tsSec {
		return 1, fmt.Errorf("ts %d not find in data (ts start at %d)", tsSec, candles.T[0])
	}
	j := 0
	for ; j < len(candles.T); j++ {
		if candles.T[j]+5*60 > tsSec {
			break
		}
	}
	if j == len(candles.T) {
		return 1, fmt.Errorf("ts %d not find in data (last ts at %d)", tsSec, candles.T[len(candles.T)-1])
	}
	return candles.O[j], nil
}

// pythHistory gets candles from the historical benchmark api
// for the indices defined in uni_pyth
func pythHistory(pythSyms []string, fromMs, toMs int64) (map[string]*utils.PythHistoryAPIResponse, error) {

	baseUrl := "https://benchmarks.pyth.network/"
	capacity := 20
	refillRate := 5.0
	tb := utils.NewTokenBucket(capacity, refillRate)
	var cdl utils.PythCandleResolution
	cdl.New(5, utils.MinuteCandle)
	hist := make(map[string]*utils.PythHistoryAPIResponse)
	for _, pythSym := range pythSyms {
		res, err := utils.GetPythHistory(
			baseUrl,
			tb,
			pythSym,
			cdl,
			uint32(fromMs/1000),
			uint32(toMs/1000),
		)
		if err != nil {
			return nil, err
		}
		if len(res.T) == 0 {
			return nil, fmt.Errorf("no pyth benchmark data found for symbol %s", pythSym)
		}
		sym := strings.Replace(strings.Split(pythSym, ".")[1], "/", "-", 1)
		hist[sym] = &res
	}
	return hist, nil
}
