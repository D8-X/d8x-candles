package pythclient

import (
	"d8x-candles/src/utils"
)

type PythClientApp struct {
	BaseUrl     string
	RedisClient *utils.RueidisClient
	TokenBucket *TokenBucket
	SymbolMngr  *utils.SymbolManager
	MsgCount    map[string]int
	StreamMngr  StreamManager
}

type PythHistoryAPIResponse struct {
	S      string    `json:"s"`      // "ok", "error"
	T      []uint32  `json:"t"`      // bar time (ts seconds, start)
	O      []float64 `json:"o"`      // open
	H      []float64 `json:"h"`      // high
	L      []float64 `json:"l"`      // low
	C      []float64 `json:"c"`      // close
	V      []float64 `json:"v"`      // volume (empty)
	ErrMsg string    `json:"errmsg"` // "" or error
}

type PriceObservations struct {
	T []uint32  `json:"t"` // time (ts seconds, start)
	P []float64 `json:"p"` // price
}

// Subscription response
// {"type":"subscribe","msg":"btc-usd:1h","data":[
// {"start":1689692400000,"time":"2023-07-18T15:00:00.000Z","open":"29822.5","high":"29981","low":"29822.5","close":"29924"},
// {"start":1689696000000,"time":"2023-07-18T16:00:00.000Z","open":"29923","high":"29932","low":"29844","close":"29906"},
// ...
// ]}
type D8XCandleResponse struct {
	Type string     `json:"type"`
	Msg  string     `json:"msg"`
	Data []OhlcData `json:"data"`
}

type OhlcData struct {
	TsMs int64   `json:"start"` // start time in milliseconds
	Time string  `json:"time"`  //e.g. "2023-07-18T15:00:00.000Z"
	O    float64 `json:"open"`
	H    float64 `json:"high"`
	L    float64 `json:"low"`
	C    float64 `json:"close"`
}
