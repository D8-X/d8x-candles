package utils

import (
	"context"
	"errors"
	"log/slog"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
)

// Compact price observations so that aggregations needed for candles remain the same
func CompactAllPriceObs(client *rueidis.Client, pxtype d8xUtils.PriceType) {
	c := *client
	key := RDS_AVAIL_TICKER_SET + ":" + pxtype.String()
	members, err := c.Do(context.Background(), c.B().Smembers().Key(key).Build()).AsStrSlice()
	if err != nil {
		slog.Error("UpdateMarketResponses:" + err.Error())
		return
	}
	for _, sym := range members {
		err := CompactPriceObs(client, sym, pxtype)
		if err != nil {
			slog.Error("Compaction failed for " + sym + ":" + err.Error())
			continue
		}
		slog.Info("Compaction succeeded for " + sym)
	}
}

// Reduce the data in REDIS for the given symbol, so that
// we are able to still display the same candles
func CompactPriceObs(client *rueidis.Client, sym string, pxtype d8xUtils.PriceType) error {
	key := pxtype.String() + ":" + sym
	info, err := (*client).Do(context.Background(), (*client).B().
		TsInfo().Key(key).Build()).AsMap()
	if err != nil {
		// key does not exist
		return errors.New("symbol not in REDIS " + key)
	}
	f := info["firstTimestamp"]
	l := info["lastTimestamp"]
	first, _ := (&f).AsInt64()
	last, _ := (&l).AsInt64()
	priceObs, err := ExtractCompactedPriceObs(client, sym, pxtype, first, last)
	if err != nil {
		return err
	}

	// add compacted data to a temporary series
	err = PricesToRedis(client, sym+"tmp", pxtype, priceObs)
	if err != nil {
		return err
	}
	// clear data
	(*client).Do(context.Background(), (*client).B().TsDel().Key(key).FromTimestamp(first).ToTimestamp(last).Build())
	// rename compacted data
	// Rename the temporary key to the original key
	keyTmp := key + "tmp"
	(*client).Do(context.Background(), (*client).B().Rename().Key(keyTmp).Newkey(key).Build())
	return nil
}

// Construct price observations from OHLC data sourced from REDIS,
// used to clean-up high granularity data. To do so, we retain 1 day data up to 1 month before 'last',
// 1h data for the current month, 1min data for the last 3 days
func ExtractCompactedPriceObs(rueidi *rueidis.Client, sym string, pxtype d8xUtils.PriceType, first int64, last int64) (PriceObservations, error) {
	client := *rueidi

	last1D := last - 86400000*30
	var ohlc1d, ohlc1h, ohlc1m []OhlcData
	if last1D > first {
		ohlc1d, _ = OhlcFromRedis(rueidi, sym, pxtype, first, last1D, 86400)
	}
	first1H := last1D
	last1H := last - 3*86400000
	ohlc1h, _ = OhlcFromRedis(&client, sym, pxtype, first1H, last1H, 60*60)
	first1m := last1H
	ohlc1m, _ = OhlcFromRedis(&client, sym, pxtype, first1m, last, 60)
	var candles = [][]OhlcData{ohlc1m, ohlc1h, ohlc1d}
	var obs PriceObservations
	obs, err := OhlcCandlesToPriceObs(candles, sym)
	if err != nil {
		return PriceObservations{}, err
	}
	return obs, nil
}
