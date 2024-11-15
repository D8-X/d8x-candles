package polyclient

import (
	"d8x-candles/src/utils"
	"fmt"
	"log/slog"
	"testing"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/spf13/viper"
)

func loadRedisCredentials() (string, string) {
	viper.SetConfigFile("../../.env")
	if err := viper.ReadInConfig(); err != nil {
		slog.Error("could not load .env file" + err.Error())
	}
	return viper.GetString("REDIS_ADDR"), viper.GetString("REDIS_PW")
}

func loadStorkCredentials() (string, string) {
	viper.SetConfigFile("../../.env")
	if err := viper.ReadInConfig(); err != nil {
		slog.Error("could not load .env file" + err.Error())
	}
	return viper.GetString("STORK_CREDENTIALS"), viper.GetString("STORK_ENDPOINT")
}

func TestPolyClient(t *testing.T) {
	// run this test and as a separate instance
	// go run cmd/ws-server/main.go
	// and redis
	// then connect to the websocket like 127.0.0.1:8081/ws with postman or so
	// and send a subscription:
	//{
	//	"type": "subscribe",
	//	"topic": "EL24-usd:1M"
	//}
	config := []d8xUtils.PriceFeedId{
		{
			Symbol: "EL24-USD",
			Id:     "0x3011e4ede0f6befa0ad3f571001d3e1ffeef3d4af78c3112aaac90416e3a43e7",
			Type:   utils.TYPE_POLYMARKET.ToString(),
			Origin: "0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917",
		},
	}
	s0, s1 := loadStorkCredentials()
	r0, r1 := loadRedisCredentials()
	if r0 == "" {
		slog.Info("REDIS credentials needed in .env file")
		t.FailNow()
	}
	app, err := NewPolyClient("https://odin-poly.d8x.xyz", r0, r1, s0, s1, config)
	if err != nil {
		fmt.Println("error:", err.Error())
		t.FailNow()
	}
	err = app.Run()
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestPolyClient2(t *testing.T) {
	// run this test and as a separate instance
	// go run cmd/ws-server/main.go
	// and redis
	// then connect to the websocket like 127.0.0.1:8081/ws with postman or so
	// and send a subscription:
	//{
	//	"type": "subscribe",
	//	"topic": "trump-usd:1M"
	//}

	config := []d8xUtils.PriceFeedId{
		{
			Symbol: "trump-usd",
			Id:     "0x3011e4ede0f6befa0ad3f571001d3e1ffeef3d4af78c3112aaac90416e3a43e7",
			Type:   utils.TYPE_POLYMARKET.ToString(),
			Origin: "0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917",
		},
	}
	/*
		config := []d8xUtils.PriceFeedId{
			{
				Symbol: "mark-usd",
				Id:     "0x7F8D2BDA628CEE500C6CF3998542F0A80B9DC38090CE0CDD7742A26755819BE9",
				Type:   utils.POLYMARKET_TYPE.ToString(),
				Origin: "0xd83c13f14529750b296d6d2b327cbcadd34c2c3e673f4242c1be94d462a36617",
			}}
	*/
	r0, r1 := loadRedisCredentials()
	if r0 == "" {
		slog.Info("REDIS credentials needed in .env file")
		t.FailNow()
	}
	s0, s1 := loadStorkCredentials()
	app, err := NewPolyClient("https://odin-poly.d8x.xyz", r0, r1, s0, s1, config)
	if err != nil {
		fmt.Println("error:", err.Error())
		t.FailNow()
	}
	err = app.Run()
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestMktInfoUpdate(t *testing.T) {
	// ensure Redis is up for this test
	config := []d8xUtils.PriceFeedId{
		{
			Symbol: "EL24-USD",
			Id:     "0x3011e4ede0f6befa0ad3f571001d3e1ffeef3d4af78c3112aaac90416e3a43e7",
			Type:   utils.TYPE_POLYMARKET.ToString(),
			Origin: "0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917",
		},
	}
	r0, r1 := loadRedisCredentials()
	if r0 == "" {
		slog.Info("REDIS credentials needed in .env file")
		t.FailNow()
	}
	s0, s1 := loadStorkCredentials()
	app, err := NewPolyClient("https://odin-poly.d8x.xyz", r0, r1, s0, s1, config)
	if err != nil {
		fmt.Println("error:", err.Error())
		t.FailNow()
	}
	app.FetchMktInfo([]string{"EL24-USD"})
	// check REDIS
	m, err := utils.RedisGetMarketInfo(app.RedisClient.Ctx, app.RedisClient.Client, "EL24-USD")
	if err != nil {
		fmt.Println("error:", err.Error())
		t.FailNow()
	}
	fmt.Println(m)
}
