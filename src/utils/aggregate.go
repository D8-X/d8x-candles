package utils

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

func ConvertTimestampToISO8601(timestampMs int64) string {
	timestamp := time.Unix(0, timestampMs*int64(time.Millisecond))
	iso8601 := timestamp.UTC().Format("2006-01-02T15:04:05.000Z")
	return iso8601
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
func OhlcCandlesToPriceObs(candles [][]OhlcData, buckets []int64, sym string) (PriceObservations, error) {
	var px PriceObservations
	stopAtTs := int64(0)
	nextLow := float64(0)
	nextHigh := float64(0)
	for i := 0; i < len(candles); i++ {
		err := ohlcToPriceObs(&px, candles[i], buckets[i], stopAtTs, nextLow, nextHigh)
		if err != nil {
			slog.Info(fmt.Sprintf("Some candle not available for %s: %s", sym, err.Error()))
			continue
		}
		if len(px.P) > 0 {
			stopAtTs = int64(px.T[0])
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
func ohlcToPriceObs(px *PriceObservations, candles []OhlcData, candleResolutionSec int64, stopAtTs int64, nextLow float64, nextHigh float64) error {
	if len(candles) < 2 {
		return errors.New("less than 2 candle observations")
	}

	for t := 0; t < len(candles); t++ {
		timeSec := (candles[t].TsMs / 1000)
		//open
		px.T = append(px.T, uint32(timeSec))
		px.P = append(px.P, candles[t].O)

		if stopAtTs != 0 && timeSec+candleResolutionSec >= stopAtTs {
			// handle last element
			modResolution := stopAtTs - timeSec
			if candles[t].H > nextHigh {
				// we need to place High
				px.T = append(px.T, uint32(timeSec+modResolution/4*3))
				px.P = append(px.P, candles[t].H)
			}
			if candles[t].L < nextLow {
				// we need to place Low
				px.T = append(px.T, uint32(timeSec+modResolution/4*3))
				px.P = append(px.P, candles[t].L)
			}
			break
		}
		//low
		px.T = append(px.T, uint32(timeSec+candleResolutionSec/4))
		px.P = append(px.P, candles[t].L)
		//high
		px.T = append(px.T, uint32(timeSec+candleResolutionSec/4*3))
		px.P = append(px.P, candles[t].H)
		//close
		px.T = append(px.T, uint32(timeSec+candleResolutionSec-1))
		px.P = append(px.P, candles[t].C)
	}
	return nil
}
