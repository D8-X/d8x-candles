package builder

import (
	"d8x-candles/src/utils"
	"fmt"
	"time"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
)

// Ohlc queries OHLC data from REDIS price cache, timestamps in ms
// sym is of the form btc-usd
func Ohlc(client *redistimeseries.Client, sym string, fromTs int64, toTs int64, resolSec uint32) ([]OhlcData, error) {
	agg := redistimeseries.DefaultRangeOptions
	agg.TimeBucket = int(resolSec) * 1000
	//agg.Count = 100
	// collect aggregations
	aggregations := []redistimeseries.AggregationType{redistimeseries.FirstAggregation,
		redistimeseries.MaxAggregation,
		redistimeseries.MinAggregation,
		redistimeseries.LastAggregation}

	var redisData []*[]redistimeseries.DataPoint
	for k := 0; k < len(aggregations); k++ {
		agg.AggType = aggregations[k]
		data0, err := client.Range(sym, fromTs, toTs)
		fmt.Print(data0)
		data, err := client.RangeWithOptions(sym, fromTs, toTs, agg)

		if err != nil {
			return []OhlcData{}, err
		}
		redisData = append(redisData, &data)
	}

	// store in candle format
	var ohlc []OhlcData
	// start at one because redis aggregation set 0 for open
	for k := 1; k < len(*redisData[0]); k++ {
		var data OhlcData
		data.StartTsMs = (*redisData[0])[k].Timestamp
		data.Time = ConvertTimestampToISO8601(data.StartTsMs)
		data.O = (*redisData[0])[k].Value
		data.H = (*redisData[1])[k].Value
		data.L = (*redisData[2])[k].Value
		data.C = (*redisData[3])[k].Value
		ohlc = append(ohlc, data)
	}
	return ohlc, nil
}

func ConvertTimestampToISO8601(timestampMs int64) string {
	timestamp := time.Unix(0, timestampMs*int64(time.Millisecond))
	iso8601 := timestamp.UTC().Format("2006-01-02T15:04:05.000Z")
	return iso8601
}

func AddPriceObs(client *redistimeseries.Client, sym utils.SymbolPyth, timestampMs int64, value float64) {
	client.Add(sym.Symbol, timestampMs, value)
}

func CreateTimeSeries(client *redistimeseries.Client, sym utils.SymbolPyth) {
	var keyname = sym.Symbol
	a, haveit := client.Info(keyname)
	fmt.Print(a)
	if haveit == nil {
		// key exists, we purge the timeseries
		client.DeleteSerie(keyname)
	}
	// key does not exist, create series
	client.CreateKeyWithOptions(keyname, redistimeseries.DefaultCreateOptions)
}
