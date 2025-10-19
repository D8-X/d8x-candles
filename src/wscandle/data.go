package wscandle

import (
	"log/slog"
	"time"

	"d8x-candles/src/utils"

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
	if p.DisplayRangeMs == 0 {
		fromTsMs = tMs - 86400*365*1000
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
