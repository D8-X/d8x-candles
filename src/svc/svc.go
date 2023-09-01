package svc

import (
	"d8x-candles/env"
	"d8x-candles/src/pythclient"
	"d8x-candles/src/utils"
	"d8x-candles/src/wscandle"
	"fmt"
	"log/slog"

	"github.com/spf13/viper"
)

func RunCandleCharts() {
	err := loadEnv()
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
	wscandle.StartWSServer()
}

func StreamPyth() {
	//wss://hermes-beta.pyth.network/ws
	err := loadEnv()
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
	c, err := loadConfig()
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}

	err = pythclient.StreamWs(c)
	if err != nil {
		slog.Error(err.Error())
	}
}

func loadConfig() (utils.PriceConfig, error) {
	fileName := viper.GetString(env.CONFIG_PATH)
	var c utils.PriceConfig
	err := c.LoadPriceConfig(fileName)
	if err != nil {
		return utils.PriceConfig{}, err
	}
	return c, nil
}

func loadEnv() error {

	viper.SetConfigFile(".env")
	if err := viper.ReadInConfig(); err != nil {
		slog.Error("could not load .env file", err)
	}

	viper.SetDefault(env.PYTH_API_BASE_URL, "https://benchmarks.pyth.network/")

	requiredEnvs := []string{}

	for _, e := range requiredEnvs {
		if !viper.IsSet(e) {
			return fmt.Errorf("required environment variable not set", e)
		}
	}
	return nil
}
