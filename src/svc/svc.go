package svc

import (
	"d8x-candles/env"
	"d8x-candles/src/polyclient"
	"d8x-candles/src/pythclient"
	"d8x-candles/src/utils"
	"d8x-candles/src/wscandle"
	"errors"
	"fmt"
	"log/slog"
	"os"

	d8xConf "github.com/D8-X/d8x-futures-go-sdk/config"
	"github.com/spf13/viper"
)

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
	})))
}

func RunCandleCharts() {
	err := loadEnv([]string{
		env.CONFIG_PATH,
	})
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

func StreamPolyMarkets() {
	err := loadEnv([]string{
		env.CONFIG_PATH,
	})
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
	config, err := d8xConf.GetDefaultPriceConfig(42161) //chain-id irrellevant since same config
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
	found := false
	for _, el := range config.PriceFeedIds {
		if el.Type == utils.POLYMARKET_TYPE {
			found = true
			break
		}
	}
	if !found {
		fmt.Println("no polymarket found in configuration, quitting")
		return
	}
	c, err := loadConfig()
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
	app, err := polyclient.NewPolyClient(
		c.ConfigFile.PredMktPriceEndpoints[0],
		viper.GetString(env.REDIS_ADDR),
		viper.GetString(env.REDIS_PW),
		viper.GetString(env.STORK_ENDPOINT),
		viper.GetString(env.STORK_CREDENTIALS),
		config.PriceFeedIds)

	if err != nil {
		fmt.Println("Error:", err.Error())
		panic(err)
	}
	err = app.Run()
	if err != nil {
		fmt.Println(err.Error())
	}
	panic(fmt.Errorf("terminated"))
}

func StreamPyth() {
	err := loadEnv([]string{
		env.CONFIG_PATH,
	})
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
	c, err := loadConfig()
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}

	err = pythclient.Run(&c, viper.GetString(env.REDIS_ADDR), viper.GetString(env.REDIS_PW))
	if err != nil {
		slog.Error(err.Error())
	}
}

func loadConfig() (utils.SymbolManager, error) {
	fileName := viper.GetString(env.CONFIG_PATH)
	var c utils.SymbolManager
	err := c.New(fileName)
	if err != nil {
		return utils.SymbolManager{}, err
	}
	return c, nil
}

func loadEnv(requiredEnvs []string) error {
	viper.SetConfigFile(".env")
	if err := viper.ReadInConfig(); err != nil {
		slog.Error("could not load .env file" + err.Error())
	}

	viper.AutomaticEnv()

	viper.SetDefault(env.PYTH_API_BASE_URL, "https://benchmarks.pyth.network/")
	viper.SetDefault(env.REDIS_ADDR, "localhost:6379")
	viper.SetDefault(env.WS_ADDR, "localhost:8080")
	viper.SetDefault(env.REDIS_DB_NUM, 0)
	for _, e := range requiredEnvs {
		if !viper.IsSet(e) {
			return errors.New("required environment variable not set" + e)
		}
	}

	return nil
}
