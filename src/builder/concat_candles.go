package builder

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
// decreasing resolution (e.g., 1 minute first, 5 minute next, 1h next, 1 day next)
func CandlesToPriceObs(candles []PythHistoryAPIResponse) (PriceObservations, error) {
	var px PriceObservations
	var stopAtTs = uint32(0)
	var nextLow = float64(0)
	var nextHigh = float64(0)
	for i := 0; i < len(candles); i++ {
		candleToPriceObs(&px, candles[i], stopAtTs, nextLow, nextHigh)
		if len(px.P) > 0 {
			stopAtTs = px.T[0]
			nextHigh = candles[i].H[0]
			nextLow = candles[i].L[0]
		}
	}
	return px, nil
}

// Produce artificial price observations from candle data.
// Only process the candle data so that the last candle is the first to end after stopAtTs
// nextLow and nextHigh are the LH values of the first candle that has a higher resolution and starts
// at stopAtTs
func candleToPriceObs(px *PriceObservations, candles PythHistoryAPIResponse, stopAtTs uint32, nextLow float64, nextHigh float64) {
	if len(candles.T) < 2 {
		return
	}
	// determine resolution (seconds)
	candleResolution := candles.T[1] - candles.T[0]
	// we place open to candles.T
	//			low to candles.T+candleResolution*0.25
	//          high to candles.T+candleResolution*0.75
	//          close to candles.T+candleResolution
	// assuming open=previous close

	// initial open
	px.T = append(px.T, candles.T[0])
	px.P = append(px.P, candles.O[0])
	for i := 0; i < len(candles.T); i++ {
		if stopAtTs != 0 && candles.T[i]+candleResolution >= stopAtTs {
			// handle last element
			modResolution := stopAtTs - candles.T[i]
			if candles.H[i] > nextHigh {
				// we need to place High
				px.T = append(px.T, candles.T[i]+modResolution/4*3)
				px.P = append(px.P, candles.H[i])
			}
			if candles.L[i] < nextLow {
				// we need to place Low
				px.T = append(px.T, candles.T[i]+modResolution/4*3)
				px.P = append(px.P, candles.L[i])
			}
			break
		}
		//low
		px.T = append(px.T, candles.T[i]+candleResolution/4)
		px.P = append(px.P, candles.L[i])
		//high
		px.T = append(px.T, candles.T[i]+candleResolution/4*3)
		px.P = append(px.P, candles.H[i])
		//close
		px.T = append(px.T, candles.T[i]+candleResolution)
		px.P = append(px.P, candles.C[i])
	}
}
