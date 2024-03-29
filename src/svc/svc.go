package svc

import (
	"d8x-candles/env"
	"d8x-candles/src/pythclient"
	"d8x-candles/src/utils"
	"d8x-candles/src/wscandle"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/viper"
)

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
	})))
}

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

	err = pythclient.StreamWs(&c, viper.GetString(env.REDIS_ADDR), viper.GetString(env.REDIS_PW))
	if err != nil {
		slog.Error(err.Error())
	}
}

func loadConfig() (utils.SymbolManager, error) {
	fileName := viper.GetString(env.CONFIG_PATH)
	network := viper.GetString(env.NETWORK_NAME)
	var c utils.SymbolManager
	err := c.New(fileName, network)
	if err != nil {
		return utils.SymbolManager{}, err
	}
	return c, nil
}

func loadEnv() error {
	viper.SetConfigFile(".env")
	if err := viper.ReadInConfig(); err != nil {
		slog.Error("could not load .env file" + err.Error())
	}

	viper.AutomaticEnv()

	viper.SetDefault(env.PYTH_API_BASE_URL, "https://benchmarks.pyth.network/")
	viper.SetDefault(env.REDIS_ADDR, "localhost:6379")
	viper.SetDefault(env.WS_ADDR, "localhost:8080")
	viper.SetDefault(env.REDIS_DB_NUM, 0)
	viper.SetDefault(env.NETWORK_NAME, "testnet")
	requiredEnvs := []string{
		env.CONFIG_PATH,
	}

	for _, e := range requiredEnvs {
		if !viper.IsSet(e) {
			return errors.New("required environment variable not set" + e)
		}
	}

	return nil
}
