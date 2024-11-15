package utils

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

// Ohlc queries OHLC data from REDIS price cache, timestamps in ms
// sym is of the form btc-usd
func Ohlc(client *rueidis.Client, sym string, pxtype PriceType, fromTs int64, toTs int64, resolSec uint32) ([]OhlcData, error) {

	timeBucket := int64(resolSec) * 1000
	//agg.Count = 100
	// collect aggregations
	aggregations := []string{"first", "max", "min", "last"}

	var redisData []*[]DataPoint
	for _, a := range aggregations {
		data, err := RangeAggr(client, sym, pxtype, fromTs, toTs, timeBucket, a)
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

func RangeAggr(r *rueidis.Client, sym string, pxtype PriceType, fromTs int64, toTs int64, bucketDur int64, aggr string) ([]DataPoint, error) {
	key := pxtype.ToString() + ":" + sym
	var cmd rueidis.Completed
	fromTs = int64(fromTs/bucketDur) * bucketDur
	switch aggr {
	case "min":
		cmd = (*r).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationMin().Bucketduration(bucketDur).Build()
	case "max":
		cmd = (*r).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationMax().Bucketduration(bucketDur).Build()
	case "first":
		cmd = (*r).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationFirst().Bucketduration(bucketDur).Build()
	case "last":
		cmd = (*r).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationLast().Bucketduration(bucketDur).Build()
	case "": //no aggregation
		cmd = (*r).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			Build()
	default:
		return []DataPoint{}, errors.New("invalid aggr type")
	}
	raw, err := (*r).Do(context.Background(), cmd).ToAny()
	if err != nil {
		return []DataPoint{}, err
	}
	data := ParseTsRange(raw)

	return data, nil
}

func ParseTsRange(data interface{}) []DataPoint {
	rSlice, ok := data.([]interface{})
	if !ok {
		return []DataPoint{}
	}

	dataPoints := make([]DataPoint, len(rSlice))
	for k, innerSlice := range rSlice {
		if inner, ok := innerSlice.([]interface{}); ok && len(inner) == 2 {
			if intValue, ok := inner[0].(int64); ok {
				dataPoints[k].Timestamp = intValue
			}
			if floatValue, ok := inner[1].(float64); ok {
				dataPoints[k].Value = floatValue
			}
		}
	}
	return dataPoints
}

// Analogue to PythCandlesToPriceObs
func OhlcCandlesToPriceObs(candles [][]OhlcData, sym string) (PriceObservations, error) {
	var px PriceObservations
	var stopAtTs = uint32(0)
	var nextLow = float64(0)
	var nextHigh = float64(0)
	for i := 0; i < len(candles); i++ {
		err := ohlcToPriceObs(&px, candles[i], stopAtTs, nextLow, nextHigh)
		if err != nil {
			slog.Info(fmt.Sprintf("Some candle not available for %s: %s", sym, err.Error()))
			continue
		}
		if len(px.P) > 0 {
			stopAtTs = px.T[0]
			nextHigh = candles[i][0].H
			nextLow = candles[i][0].L
		}
	}
	return px, nil
}

// Produce artificial price observations from ohlc candle data, used e.g. when constructing triangulations
// from candles.
// Identical to candleToPriceObs we process the candle data so that the last candle is
// the first to end after stopAtTs
// nextLow and nextHigh are the LH values of the first candle that has a higher resolution and starts
// at stopAtTs (seconds)
func ohlcToPriceObs(px *PriceObservations, candles []OhlcData, stopAtTs uint32, nextLow float64, nextHigh float64) error {
	if len(candles) < 2 {
		return errors.New("less than 2 candle observations")
	}
	// determine resolution (sec)
	candleResolutionSec := uint32((candles[1].TsMs - candles[0].TsMs) / 1000)

	for t := 0; t < len(candles); t++ {
		var timeSec uint32 = uint32(candles[t].TsMs / 1000)
		//open
		px.T = append(px.T, timeSec)
		px.P = append(px.P, candles[t].O)

		if stopAtTs != 0 && timeSec+candleResolutionSec >= stopAtTs {
			// handle last element
			modResolution := stopAtTs - timeSec
			if candles[t].H > nextHigh {
				// we need to place High
				px.T = append(px.T, timeSec+modResolution/4*3)
				px.P = append(px.P, candles[t].H)
			}
			if candles[t].L < nextLow {
				// we need to place Low
				px.T = append(px.T, timeSec+modResolution/4*3)
				px.P = append(px.P, candles[t].L)
			}
			break
		}
		//low
		px.T = append(px.T, timeSec+candleResolutionSec/4)
		px.P = append(px.P, candles[t].L)
		//high
		px.T = append(px.T, timeSec+candleResolutionSec/4*3)
		px.P = append(px.P, candles[t].H)
		//close
		px.T = append(px.T, timeSec+candleResolutionSec-1)
		px.P = append(px.P, candles[t].C)
	}
	return nil
}
