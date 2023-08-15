package builder

type PythHistoryAPI struct {
	BaseUrl string
}

type PythHistoryAPIResponse struct {
	S string    `json:"s"` // "ok"
	T []uint32  `json:"t"` // bar time (ts seconds, start)
	O []float64 `json:"o"` // open
	H []float64 `json:"h"` // high
	L []float64 `json:"l"` // low
	C []float64 `json:"c"` // close
	V []float64 `json:"v"` // volume (empty)
}

type PriceObservations struct {
	T []uint32  `json:"t"` // time (ts seconds, start)
	P []float64 `json:"p"` // price
}
