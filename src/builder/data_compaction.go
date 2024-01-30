package builder

import (
	"errors"
	"log/slog"
	"time"
)

// Schedule regular calls of compaction (e.g. every hour)
func (p *PythHistoryAPI) ScheduleCompaction(waitTime time.Duration) {
	tickerUpdate := time.NewTicker(waitTime)
	for {
		select {
		case <-tickerUpdate.C:
			p.CompactAllPriceObs()
			slog.Info("Compaction completed.")
		}
	}
}

// Compact price observations so that aggregations needed for candles remain the same
func (p *PythHistoryAPI) CompactAllPriceObs() {
	config := p.SymbolMngr
	for _, sym := range config.PythIdToSym {
		err := p.CompactPriceObs(sym)
		if err != nil {
			slog.Error("Compaction failed for " + sym + ":" + err.Error())
			continue
		}
		slog.Info("Compaction succeeded for " + sym)
	}
	for _, triang := range config.ConfigFile.Triangulations {
		err := p.CompactPriceObs(triang.Target)
		if err != nil {
			slog.Error("Compaction failed for " + triang.Target + ":" + err.Error())
			continue
		}
		slog.Info("Compaction succeeded for " + triang.Target)
	}
}

// Reduce the data in REDIS for the given symbol, so that
// we are able to still display the same candles
func (p *PythHistoryAPI) CompactPriceObs(sym string) error {
	client := *p.RedisClient.Client
	info, err := (client).Do(p.RedisClient.Ctx, client.B().
		TsInfo().Key(sym).Build()).AsMap()
	if err != nil {
		// key does not exist
		return errors.New("symbol not in REDIS" + sym)
	}
	f := info["firstTimestamp"]
	l := info["lastTimestamp"]
	first, _ := (&f).AsInt64()
	last, _ := (&l).AsInt64()
	priceObs, err := p.ExtractCompactedPriceObs(sym, first, last)
	if err != nil {
		return err
	}
	// clear data
	client.Do(p.RedisClient.Ctx, client.B().TsDel().Key(sym).FromTimestamp(first).ToTimestamp(last).Build())
	// add compacted data
	p.PricesToRedis(sym, priceObs)
	return nil
}

// Construct price observations from OHLC data sourced from REDIS,
// used to clean-up high granularity data. To do so, we retain 1 day data up to 1 month before 'last',
// 1h data for the current month, 1min data for the last 3 days
func (p *PythHistoryAPI) ExtractCompactedPriceObs(sym string, first int64, last int64) (PriceObservations, error) {
	client := p.RedisClient

	last1D := last - 86400000*30
	var ohlc1d, ohlc1h, ohlc1m []OhlcData
	if last1D > first {
		ohlc1d, _ = Ohlc(client, sym, first, last1D, 86400)
	}
	first1H := last1D
	last1H := last - 3*86400000
	ohlc1h, _ = Ohlc(client, sym, first1H, last1H, 60*60)
	first1m := last1H
	ohlc1m, _ = Ohlc(client, sym, first1m, last, 60)
	var candles = [][]OhlcData{ohlc1m, ohlc1h, ohlc1d}
	var obs PriceObservations
	obs, err := OhlcCandlesToPriceObs(candles)
	if err != nil {
		return PriceObservations{}, err
	}
	return obs, nil
}
