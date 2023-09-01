package builder

import (
	"d8x-candles/src/utils"
	"fmt"
	"testing"
	"time"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
)

func TestRetrieveCandle(t *testing.T) {
	api := PythHistoryAPI{BaseUrl: "https://benchmarks.pyth.network/"}
	var sym utils.SymbolPyth
	sym.New("Crypto.ETH/USD", "")
	var resol utils.PythCandleResolution
	err := resol.New(1, utils.MinuteCandle)
	if err != nil {
		t.Errorf("Wrong time resolution:%v", err)
		return
	}

	fromTs, err := timestampFromTimeString("2023-08-14 08:20")
	if err != nil {
		t.Errorf("Error parsing date:%v", err)
		return
	}
	toTs, err := timestampFromTimeString("2023-08-14 08:35")
	if err != nil {
		t.Errorf("Error parsing date:%v", err)
		return
	}
	res, err := api.RetrieveCandlesFromPyth(sym, resol, fromTs, toTs)
	if err != nil {
		t.Errorf("Error parsing date:%v", err)
		return
	}
	fmt.Print(res)
}

func TestConcatCandles(t *testing.T) {
	fromTs, _ := timestampFromTimeString("2023-05-14 08:20")
	toTs, _ := timestampFromTimeString("2023-08-14 08:35")
	api := PythHistoryAPI{BaseUrl: "https://benchmarks.pyth.network/"}
	var sym utils.SymbolPyth
	//sym.New("Crypto.ETH/USD")
	sym.New("Fx.USD/CHF", "")
	var resol utils.PythCandleResolution
	_ = resol.New(1, utils.MinuteCandle)
	candles1min, err := api.RetrieveCandlesFromPyth(sym, resol, fromTs, toTs)
	if err != nil {
		t.Errorf("Error retrieving candles:%v", err)
		return
	}
	_ = resol.New(60, utils.MinuteCandle)
	candles1h, err := api.RetrieveCandlesFromPyth(sym, resol, fromTs, toTs)
	if err != nil {
		t.Errorf("Error retrieving candles:%v", err)
		return
	}
	var candles = []PythHistoryAPIResponse{candles1min, candles1h}
	p, err := CandlesToPriceObs(candles)
	if err != nil {
		t.Errorf("Error parsing date:%v", err)
		return
	}
	fmt.Print(p)
}

func TestPythDataToRedisPriceObs(t *testing.T) {
	host := "localhost:6379"
	//password := ""
	var client = redistimeseries.NewClient(host, "client", nil)
	api := PythHistoryAPI{BaseUrl: "https://benchmarks.pyth.network/", RedisClient: client}
	var sym1, sym2 utils.SymbolPyth
	sym1.New("Crypto.ETH/USD", "")
	sym2.New("Fx.USD/CHF", "")
	symbols := []utils.SymbolPyth{sym1, sym2}
	api.PythDataToRedisPriceObs(symbols)
	vlast, err := client.Get(sym1.PairString())
	if err != nil {
		t.Errorf("Error parsing date:%v", err)
		return
	}
	fmt.Print(vlast)
}

func timestampFromTimeString(timestr string) (uint32, error) {
	layout := "2006-01-02 15:04"
	ts, err := time.Parse(layout, timestr)
	if err != nil {
		return 0, fmt.Errorf("Error parsing date: %v", err)
	}
	return uint32(ts.UTC().Unix()), nil
}
