package pythclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"d8x-candles/src/utils"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
	"github.com/spf13/viper"
)

func TestRetrieveCandle(t *testing.T) {
	api := PythClientApp{BaseUrl: "https://benchmarks.pyth.network/"}
	var sym utils.SymbolPyth
	sym.New("Crypto.ETH/USD", "", "ETH/USD")
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

func formatTimestamp(ts int64) string {
	t := time.Unix(ts, 0).UTC()
	return t.Format("2006-01-02 15:04:05 MST")
}

func TestConcatCandles(t *testing.T) {
	toTs := uint32(time.Now().Unix())
	fromTs := toTs - 3.15e7 // timestampFromTimeString("2025-03-25 08:00")
	api := PythClientApp{BaseUrl: "https://benchmarks.pyth.network/"}
	api.TokenBucket = utils.NewTokenBucket(10, 5)
	var sym utils.SymbolPyth
	sym.New("Crypto.BTC/USD", "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43", "BTC/USD")
	// sym.New("Equity.US.NDAQ/USD", "", "NDAQ/USD")
	var resol utils.PythCandleResolution
	_ = resol.New(1, utils.MinuteCandle)
	candles1min, err := api.RetrieveCandlesFromPyth(sym, resol, toTs-86400, toTs)
	if err != nil {
		t.Errorf("Error retrieving candles:%v", err)
		return
	}
	_ = resol.New(30, utils.MinuteCandle)
	candles30min, err := api.RetrieveCandlesFromPyth(sym, resol, toTs-86400*5, toTs)
	if err != nil {
		t.Errorf("Error retrieving candles:%v", err)
		return
	}

	for j := 1; j < len(candles30min.T); j++ {
		t0 := candles30min.T[j-1]
		t1 := candles30min.T[j]
		if t0 > t1 {
			fmt.Printf("candles not sorted: %d %d -> diff %d", t0, t1, t1-t0)
		}
	}
	_ = resol.New(1, utils.DayCandle)
	candles24h, err := api.RetrieveCandlesFromPyth(sym, resol, fromTs, toTs)
	if err != nil {
		t.Errorf("Error retrieving candles:%v", err)
		return
	}

	candles := []utils.PythHistoryAPIResponse{candles1min, candles30min, candles24h}
	for j := range candles {
		fmt.Printf("dur = %d\n", candles[j].T[1]-candles[j].T[0])
		fmt.Printf("from %s\n", formatTimestamp(int64(candles[j].T[0])))
		fmt.Printf("to   %s\n", formatTimestamp(int64(candles[j].T[len(candles[j].T)-1])))
	}

	p, err := PythCandlesToPriceObs(candles, []int{60, 3600, 86400})
	if err != nil {
		t.Errorf("Error parsing date:%v", err)
		return
	}

	for j := 1; j < len(p.T); j++ {
		if p.T[j] < p.T[j-1] {
			fmt.Printf("j=%d: %s > %s candles not sorted: %d %d -> diff %d\n",
				j, formatTimestamp(int64(p.T[j-1])), formatTimestamp(int64(p.T[j])), p.T[j-1], p.T[j], p.T[j]-p.T[j-1])
		}
	}

	J, err := json.MarshalIndent(p, "", " ")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	os.WriteFile("./test2.json", J, 0o644)
}

func TestPythDataToRedisPriceObs(t *testing.T) {
	api := createHistApi(t)
	var sym1, sym2 utils.SymbolPyth
	sym1.New("Crypto.ETH/USD", "", "ETH/USD")
	sym2.New("Fx.USD/CHF", "", "USD/CHF")
	symbols := []utils.SymbolPyth{sym1, sym2}
	api.PythDataToRedisPriceObs(symbols)
	vlast, _ := utils.RedisTsGet(api.RedisClient, sym1.Symbol, d8xUtils.PXTYPE_PYTH)
	fmt.Print(vlast)
}

func loadEnv() *viper.Viper {
	viper.SetConfigFile("../../.env")
	if err := viper.ReadInConfig(); err != nil {
		slog.Error("could not load .env file" + err.Error())
	}
	return viper.GetViper()
}

func createHistApi(t *testing.T) PythClientApp {
	v := loadEnv()
	REDIS_ADDR := v.GetString("REDIS_ADDR")
	REDIS_PW := v.GetString("REDIS_PW")
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{REDIS_ADDR}, Password: REDIS_PW})
	if err != nil {
		t.Errorf("Error :%v", err)
		t.FailNow()
	}
	capacity := 30
	refillRate := 3.0 // 3 tokens per second
	tb := utils.NewTokenBucket(capacity, refillRate)
	api := PythClientApp{
		BaseUrl:     "https://benchmarks.pyth.network/",
		RedisClient: &client,
		TokenBucket: tb,
	}
	return api
}

func timestampFromTimeString(timestr string) (uint32, error) {
	layout := "2006-01-02 15:04"
	ts, err := time.Parse(layout, timestr)
	if err != nil {
		return 0, fmt.Errorf("Error parsing date: %v", err)
	}
	return uint32(ts.UTC().Unix()), nil
}

func TestQueryPriceFeedInfo(t *testing.T) {
	api := createHistApi(t)
	api.QueryPriceFeedInfo("ETH-USD", "", "0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace")
	r, err := api.GetMarketInfo("ETH-USD")
	if err != nil {
		t.Errorf("Error parsing date:%v", err)
		return
	}
	fmt.Print(r)
}

func TestFetchMktInfo(t *testing.T) {
	api := createHistApi(t)
	api.FetchMktInfo([]string{"chf-usdc"})
	a, err := utils.RedisGetMarketInfo(context.Background(), api.RedisClient, "CHF-USDC")
	if err != nil {
		panic(err)
	}
	fmt.Print(a)
	_, err = utils.RedisGetMarketInfo(context.Background(), api.RedisClient, "bs-ws")
	if err != nil {
		fmt.Print("intended error" + err.Error())
	}
}
