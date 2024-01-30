package utils

import "github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"

// TriangulatedPx calculates the triangulated price, given the triangulation path
// and prices
func TriangulatedPx(path d8x_futures.Triangulation, prices map[string]float64) float64 {
	var px float64 = 1
	for k := 0; k < len(path.Symbol); k++ {
		sym := path.Symbol[k]
		if prices[sym] == 0 {
			//"cannot triangulate price not set yet for " + path[k]
			return 0
		}
		if !path.IsInverse[k] {
			px = px * prices[sym]
		} else {
			px = px / prices[sym]
		}
	}
	return px
}
