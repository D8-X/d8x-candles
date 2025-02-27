package pythclient

import "d8x-candles/src/utils"

// Subscription response
// {"type":"subscribe","msg":"btc-usd:1h","data":[
// {"start":1689692400000,"time":"2023-07-18T15:00:00.000Z","open":"29822.5","high":"29981","low":"29822.5","close":"29924"},
// {"start":1689696000000,"time":"2023-07-18T16:00:00.000Z","open":"29923","high":"29932","low":"29844","close":"29906"},
// ...
// ]}
type D8XCandleResponse struct {
	Type string           `json:"type"`
	Msg  string           `json:"msg"`
	Data []utils.OhlcData `json:"data"`
}
