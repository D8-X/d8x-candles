package utils

func Triangulate(path []string, prices map[string]float64) float64 {
	var px float64 = 1
	for k := 1; k < len(path); k += 2 {
		if prices[path[k]] == 0 {
			//"cannot triangulate price not set yet for " + path[k]
			return 0
		}
		if path[k-1] == "*" {
			px = px * prices[path[k]]
		} else {
			px = px / prices[path[k]]
		}
	}
	return px
}
