package init

import (
	"d8x-candles/src/utils"
	"fmt"
	"testing"
	"time"
)

func TestRetrieveCandle(t *testing.T) {
	api := PythHistoryAPI{BaseUrl: "https://benchmarks.pyth.network/"}
	var sym utils.SymbolPyth
	sym.New("Crypto.ETH/USD")
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
	res, err := api.RetrieveCandles(sym, resol, fromTs, toTs)
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
	sym.New("Crypto.ETH/USD")
	var resol utils.PythCandleResolution
	_ = resol.New(1, utils.MinuteCandle)
	candles1min, err := api.RetrieveCandles(sym, resol, fromTs, toTs)
	if err != nil {
		t.Errorf("Error retrieving candles:%v", err)
		return
	}
	_ = resol.New(60, utils.MinuteCandle)
	candles1h, err := api.RetrieveCandles(sym, resol, fromTs, toTs)
	if err != nil {
		t.Errorf("Error retrieving candles:%v", err)
		return
	}
	var candles = []PythHistoryAPIResponse{candles1min, candles1h}
	p, err := ConcatCandles(candles)
	if err != nil {
		t.Errorf("Error parsing date:%v", err)
		return
	}
	fmt.Print(p)
}

func timestampFromTimeString(timestr string) (uint32, error) {
	layout := "2006-01-02 15:04"
	ts, err := time.Parse(layout, timestr)
	if err != nil {
		return 0, fmt.Errorf("Error parsing date: %v", err)
	}
	return uint32(ts.UTC().Unix()), nil
}
