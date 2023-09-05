package wscandle

import (
	"d8x-candles/src/builder"
	"d8x-candles/src/utils"
	"log/slog"
	"time"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
)

// construct candle stick data from redis
func GetInitialCandles(client *redistimeseries.Client, sym string, p utils.CandlePeriod) []builder.OhlcData {
	t := time.Now().UTC()
	tMs := t.UnixNano() / int64(time.Millisecond)
	tMs = (tMs / int64(p.TimeMs)) * int64(p.TimeMs) //floor
	fromTsMs := tMs - int64(p.DisplayRangeMs)
	resolSec := uint32(p.TimeMs / 1000)
	data, err := builder.Ohlc(client, sym, fromTsMs, tMs, resolSec)
	if err != nil {
		slog.Error("Error initial candles for sym " + sym)
		return []builder.OhlcData{}
	}
	return data
}
