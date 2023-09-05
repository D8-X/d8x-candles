package builder

import (
	"d8x-candles/src/utils"
	"fmt"
	"strconv"
	"time"

	"math"
)

// Ohlc queries OHLC data from REDIS price cache, timestamps in ms
// sym is of the form btc-usd
func Ohlc(client *utils.RueidisClient, sym string, fromTs int64, toTs int64, resolSec uint32) ([]OhlcData, error) {

	timeBucket := int(resolSec) * 1000
	//agg.Count = 100
	// collect aggregations
	aggregations := []string{"first", "max", "min", "last"}

	var redisData []*[]utils.DataPoint
	for _, a := range aggregations {
		data, err := client.RangeAggr(sym, fromTs, toTs, int64(timeBucket), a)
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

func AddPriceObs(client *utils.RueidisClient, sym string, timestampMs int64, value float64) {
	ts := strconv.FormatInt(timestampMs, 10)
	(*client.Client).Do(client.Ctx,
		(*client.Client).B().TsAdd().Key(sym).Timestamp(ts).Value(value).Build())
}

func CreateTimeSeries(client *utils.RueidisClient, sym string) {
	a, haveit := (*client.Client).Do(client.Ctx, (*client.Client).B().
		TsInfo().Key(sym).Build()).AsMap()
	fmt.Print(a)
	if haveit == nil {
		// key exists, we purge the timeseries
		(*client.Client).Do(client.Ctx, (*client.Client).B().TsDel().
			Key(sym).FromTimestamp(0).ToTimestamp(math.MaxInt64).Build())
	}
	// key does not exist, create series
	(*client.Client).Do(client.Ctx, (*client.Client).B().TsCreate().Key(sym).Build())
}
