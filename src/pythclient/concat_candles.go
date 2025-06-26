package pythclient

import (
	"d8x-candles/src/utils"
)

/*
Process
1 Minute -> 1 day. OK
5 minutes -> 2 days.  OK
1h -> 1 month OK
1 day -> all history
*/

// Concatenate overlapping candles that have different resolutions
// and construct price observations
// (For example 2 months of 1 day candles and 2 weeks of 1h candles)
// The function requires that the candles are sorted according to
// decreasing resolution (e.g., 1 minute first, 5 minute next, 30min next, 1 day next)
// Candles are sorted in increasing order of open time (T).
func PythCandlesToPriceObs(candles []utils.PythHistoryAPIResponse, resolutionSec []int) (utils.PriceObservations, error) {
	var px utils.PriceObservations
	startTimes := make([]uint32, len(candles))
	stopTimes := make([]uint32, len(candles))
	startTimes[len(candles)-1] = candles[len(candles)-1].T[0]
	stopTimes[0] = candles[0].T[len(candles[0].T)-1]
	for i := len(candles) - 1; i > 0; i-- {
		openTimeNext := candles[i-1].T[0]
		stopTimes[i] = openTimeNext - openTimeNext%uint32(resolutionSec[i])
		startTimes[i-1] = stopTimes[i] + uint32(resolutionSec[i])
	}
	for i := len(candles) - 1; i >= 0; i-- {
		candleToPriceObs(&px, candles[i], startTimes[i], stopTimes[i], resolutionSec[i])
	}
	return px, nil
}

// Produce artificial price observations from pyth-format candle data.
func candleToPriceObs(
	px *utils.PriceObservations,
	candles utils.PythHistoryAPIResponse,
	startAtTs uint32,
	stopAtTs uint32,
	candleResolutionSec int,
) {
	if len(candles.T) < 2 {
		return
	}
	// we place open to candles.T
	//			low to candles.T+candleResolution*0.25
	//          high to candles.T+candleResolution*0.75
	//          close to candles.T+candleResolution-1 (last slot)
	for i := 0; i < len(candles.T); i++ {
		if candles.T[i] < startAtTs {
			continue
		}
		if candles.T[i] > stopAtTs {
			break
		}
		// open
		px.T = append(px.T, candles.T[i])
		px.P = append(px.P, candles.O[i])
		// low
		px.T = append(px.T, candles.T[i]+uint32(candleResolutionSec)/4)
		px.P = append(px.P, candles.L[i])
		// high
		px.T = append(px.T, candles.T[i]+uint32(candleResolutionSec)/4*3)
		px.P = append(px.P, candles.H[i])
		// close
		px.T = append(px.T, candles.T[i]+uint32(candleResolutionSec)-1)
		px.P = append(px.P, candles.C[i])
	}
}
