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
	c, err := loadConfig()
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
	wscandle.StartWSServer(c,
		viper.GetString(env.WS_ADDR),
		viper.GetString(env.REDIS_ADDR),
		viper.GetString(env.REDIS_PW),
		viper.GetInt(env.REDIS_DB_NUM),
	)
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

	err = pythclient.StreamWs(c, viper.GetString(env.REDIS_ADDR), viper.GetString(env.REDIS_PW))
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
		slog.Error("could not load .env file", "err", err)
	}

	viper.AutomaticEnv()

	viper.SetDefault(env.PYTH_API_BASE_URL, "https://benchmarks.pyth.network/")
	viper.SetDefault(env.REDIS_ADDR, "localhost:6379")
	viper.SetDefault(env.WS_ADDR, "localhost:8080")
	viper.SetDefault(env.REDIS_DB_NUM, 0)
	requiredEnvs := []string{
		env.CONFIG_PATH,
	}

	for _, e := range requiredEnvs {
		if !viper.IsSet(e) {
			return fmt.Errorf("required environment variable not set", e)
		}
	}

	return nil
}
