package wscandle

import (
	"d8x-candles/src/builder"
	"d8x-candles/src/utils"
	"log/slog"
	"time"
)

// construct candle stick data from redis
func GetInitialCandles(client *utils.RueidisClient, sym string, p utils.CandlePeriod) []builder.OhlcData {
	t := time.Now().UTC()
	tMs := t.UnixNano() / int64(time.Millisecond)
	tMs = (tMs / int64(p.TimeMs)) * int64(p.TimeMs) //floor
	var fromTsMs int64
	if p.DisplayRangeMs == 0 {
		// all data
		a, err := (*client.Client).Do(client.Ctx, (*client.Client).B().
			TsInfo().Key(sym).Build()).AsMap()
		if err != nil {
			slog.Error("Error initial candles for sym " + sym)
			return []builder.OhlcData{}
		}
		m := a["firstTimestamp"]
		fromTsMs, _ = (&m).AsInt64()
	} else {
		fromTsMs = tMs - int64(p.DisplayRangeMs)
	}

	resolSec := uint32(p.TimeMs / 1000)
	data, err := builder.Ohlc(client, sym, fromTsMs, tMs, resolSec)
	if err != nil {
		slog.Error("Error initial candles for sym " + sym)
		return []builder.OhlcData{}
	}
	return data
}
