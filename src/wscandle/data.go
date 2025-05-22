package wscandle

import (
	"context"
	"d8x-candles/src/utils"
	"log/slog"
	"time"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
)

// construct candle stick data from redis
func GetInitialCandles(
	client *rueidis.Client,
	sym string,
	pxtype d8xUtils.PriceType,
	p utils.CandlePeriod,
) []utils.OhlcData {

	t := time.Now().UTC()
	tMs := t.UnixMilli()
	var fromTsMs int64
	key := pxtype.String() + ":" + sym
	if p.DisplayRangeMs == 0 {
		// all data
		a, err := (*client).Do(context.Background(), (*client).B().
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
	data, err := utils.OhlcFromRedis(client, sym, pxtype, fromTsMs, tMs, resolSec)
	if err != nil {
		slog.Error("Error initial candles", "symbol", sym, "error", err)
		return []utils.OhlcData{}
	}
	return data
}
