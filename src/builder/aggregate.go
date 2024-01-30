package builder

import (
	"d8x-candles/src/utils"
	"log/slog"
	"strconv"
	"time"
)

// Ohlc queries OHLC data from REDIS price cache, timestamps in ms
// sym is of the form btc-usd
func Ohlc(client *utils.RueidisClient, sym string, fromTs int64, toTs int64, resolSec uint32) ([]OhlcData, error) {

	timeBucket := int64(resolSec) * 1000
	//agg.Count = 100
	// collect aggregations
	aggregations := []string{"first", "max", "min", "last"}

	var redisData []*[]utils.DataPoint
	for _, a := range aggregations {
		data, err := client.RangeAggr(sym, fromTs, toTs, timeBucket, a)
		if err != nil {
			return []OhlcData{}, err
		}
		redisData = append(redisData, &data)
	}

	// store in candle format
	var ohlc []OhlcData
	var tOld int64 = 0
	for k := 0; k < len(*redisData[0]); k++ {
		var data OhlcData
		data.TsMs = (*redisData[0])[k].Timestamp
		data.Time = ConvertTimestampToISO8601(data.TsMs)
		data.O = (*redisData[0])[k].Value
		data.H = (*redisData[1])[k].Value
		data.L = (*redisData[2])[k].Value
		data.C = (*redisData[3])[k].Value

		// insert artificial data for gaps before adding 'data'
		numGaps := (data.TsMs - tOld) / timeBucket
		for j := 0; j < int(numGaps)-1 && k > 0; j++ {
			var dataGap OhlcData
			dataGap.TsMs = tOld + int64(j+1)*timeBucket
			dataGap.Time = ConvertTimestampToISO8601(dataGap.TsMs)
			// set all data to close of previous OHLC observation
			dataGap.O = (*redisData[3])[k].Value
			dataGap.H = (*redisData[3])[k].Value
			dataGap.L = (*redisData[3])[k].Value
			dataGap.C = (*redisData[3])[k].Value
			ohlc = append(ohlc, dataGap)
		}
		tOld = data.TsMs
		ohlc = append(ohlc, data)
	}
	return ohlc, nil
}

func ConvertTimestampToISO8601(timestampMs int64) string {
	timestamp := time.Unix(0, timestampMs*int64(time.Millisecond))
	iso8601 := timestamp.UTC().Format("2006-01-02T15:04:05.000Z")
	return iso8601
}

func AddPriceObs(client *utils.RueidisClient, sym string, timestampMs int64, value float64) error {
	ts := strconv.FormatInt(timestampMs, 10)
	resp := (*client.Client).Do(client.Ctx,
		(*client.Client).B().TsAdd().Key(sym).Timestamp(ts).Value(value).Build())
	if resp.Error() != nil {
		slog.Error("AddPriceObs " + sym + ": " + resp.Error().Error())
		return resp.Error()
	}
	return nil
}

func CreateRedisTimeSeries(client *utils.RueidisClient, sym string) {
	slog.Info("create" + sym)
	_, err := (*client.Client).Do(client.Ctx, (*client.Client).B().
		TsInfo().Key(sym).Build()).AsMap()
	if err == nil {
		// key exists, we purge the timeseries
		if err = (*client.Client).Do(client.Ctx, (*client.Client).B().Del().
			Key(sym).Build()).Error(); err != nil {
			slog.Error("CreateRedisTimeSeries, failed deleting time series " + sym + ":" + err.Error())
		}
	}
	// key does not exist, create series
	(*client.Client).Do(client.Ctx, (*client.Client).B().TsCreate().Key(sym).DuplicatePolicyLast().Build())
}
