package wscandle

import (
	"d8x-candles/src/utils"
	"log/slog"
	"time"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
)

// construct candle stick data from redis
func GetInitialCandles(
	client *utils.RueidisClient,
	sym string,
	pxtype d8xUtils.PriceType,
	p utils.CandlePeriod,
) []utils.OhlcData {

	t := time.Now().UTC()
	tMs := t.UnixNano() / int64(time.Millisecond)
	tMs = (tMs / int64(p.TimeMs)) * int64(p.TimeMs) //floor
	var fromTsMs int64
	key := pxtype.String() + ":" + sym
	if p.DisplayRangeMs == 0 {
		// all data
		a, err := (*client.Client).Do(client.Ctx, (*client.Client).B().
			TsInfo().Key(key).Build()).AsMap()
		if err != nil {
			slog.Error("Error initial candles for sym " + sym)
			return []utils.OhlcData{}
		}
		m := a["firstTimestamp"]
		fromTsMs, _ = (&m).AsInt64()
	} else {
		fromTsMs = tMs - int64(p.DisplayRangeMs)
	}

	resolSec := uint32(p.TimeMs / 1000)
	data, err := utils.OhlcFromRedis(client.Client, sym, pxtype, fromTsMs, tMs, resolSec)
	if err != nil {
		slog.Error("Error initial candles for sym " + sym)
		return []utils.OhlcData{}
	}
	return data
}
