package builder

import (
	"d8x-candles/src/utils"
	"time"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
)

// Query OHLC data from REDIS price cache
func Ohlc(client *redistimeseries.Client, sym utils.SymbolPyth, fromTs uint32, toTs uint32, resolSec uint32) ([]OhlcData, error) {
	keyname := sym.PairString()
	agg := redistimeseries.DefaultRangeOptions
	agg.TimeBucket = int(resolSec)

	// collect aggregations
	aggregations := []redistimeseries.AggregationType{redistimeseries.FirstAggregation,
		redistimeseries.MaxAggregation,
		redistimeseries.MinAggregation,
		redistimeseries.LastAggregation}

	var redisData []*[]redistimeseries.DataPoint
	for k := 0; k < len(aggregations); k++ {
		agg.AggType = aggregations[k]
		data, err := client.RangeWithOptions(keyname, int64(fromTs), int64(toTs), agg)

		if err != nil {
			return []OhlcData{}, err
		}
		redisData = append(redisData, &data)
	}

	// store in candle format
	var ohlc []OhlcData
	for k := 0; k < len(*redisData[0]); k++ {
		var data OhlcData
		data.StartTsMs = (*redisData[0])[k].Timestamp
		data.Time = convertTimestampToISO8601(data.StartTsMs)
		data.O = (*redisData[0])[k].Value
		data.H = (*redisData[1])[k].Value
		data.L = (*redisData[2])[k].Value
		data.C = (*redisData[3])[k].Value
		ohlc = append(ohlc, data)
	}
	return ohlc, nil
}

func convertTimestampToISO8601(timestampMs int64) string {
	timestamp := time.Unix(0, timestampMs*int64(time.Millisecond))
	iso8601 := timestamp.UTC().Format("2006-01-02T15:04:05.000Z")
	return iso8601
}

func AddPriceObs(client *redistimeseries.Client, sym utils.SymbolPyth, timestamp int64, value float64) {
	client.Add(sym.PairString(), timestamp, value)
}

func CreateTimeSeries(client *redistimeseries.Client, sym utils.SymbolPyth) {
	var keyname = sym.PairString()
	_, haveit := client.Info(keyname)
	if haveit == nil {
		// key exists, we purge the timeseries
		client.DeleteSerie(keyname)
	}
	// key does not exist, create series
	client.CreateKeyWithOptions(keyname, redistimeseries.DefaultCreateOptions)
}
