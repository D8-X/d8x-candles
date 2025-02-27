package svc

import (
	"d8x-candles/env"
	"d8x-candles/src/polyclient"
	"d8x-candles/src/pythclient"
	"d8x-candles/src/utils"
	"d8x-candles/src/v2client"
	"d8x-candles/src/v3client"
	"d8x-candles/src/wscandle"
	"errors"
	"fmt"
	"log/slog"
	"os"

	d8xConf "github.com/D8-X/d8x-futures-go-sdk/config"
	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
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
		env.WS_ADDR,
		env.REDIS_ADDR,
		env.REDIS_PW,
		env.REDIS_DB_NUM,
	})
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
	c, err := createSymbolMngr(viper.GetString(env.CONFIG_PATH))

	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}

	ws, err := wscandle.NewWsCandle(c.CandlePeriodsMs,
		viper.GetString(env.WS_ADDR),
		viper.GetString(env.REDIS_ADDR),
		viper.GetString(env.REDIS_PW),
		viper.GetInt(env.REDIS_DB_NUM),
	)
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
	ws.StartWSServer()
}

func RunV3Client() {
	err := loadEnv([]string{
		env.CONFIG_RPC,
		env.REDIS_ADDR,
		env.REDIS_PW,
	})
	if err != nil {
		fmt.Println("Error:", err.Error())
		panic(err)
	}

	fmt.Printf("starting V3 client for chain %d\n", viper.GetInt(env.CHAIN_ID))
	v3, err := v3client.NewV3Client(
		viper.GetString(env.CONFIG_RPC),
		viper.GetString(env.REDIS_ADDR),
		viper.GetString(env.REDIS_PW),
		viper.GetInt(env.CHAIN_ID),
		"", //we load config from remote
	)
	if err != nil {
		fmt.Println("error:", err.Error())
		panic(err)
	}
	if v3 == nil {
		// no config defined for given chain,
		// stop program
		return
	}
	err = v3.Run()
	if err != nil {
		fmt.Println("error:", err.Error())
		panic(err)
	}
}

func RunV2Client() {
	err := loadEnv([]string{
		env.CONFIG_RPC,
		env.REDIS_ADDR,
		env.REDIS_PW,
	})
	if err != nil {
		fmt.Println("Error:", err.Error())
		panic(err)
	}

	fmt.Printf("starting V2 client for chain %d\n", viper.GetInt(env.CHAIN_ID))
	v2, err := v2client.NewV2Client(
		viper.GetString(env.CONFIG_RPC),
		viper.GetString(env.REDIS_ADDR),
		viper.GetString(env.REDIS_PW),
		viper.GetInt(env.CHAIN_ID),
		"", //we load config from remote
	)
	if err != nil {
		fmt.Println("error:", err.Error())
		panic(err)
	}
	if v2 == nil {
		// no config defined for given chain,
		// stop program
		return
	}
	err = v2.Run()
	if err != nil {
		fmt.Println("error:", err.Error())
		panic(err)
	}
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
		if el.Type == d8xUtils.PXTYPE_POLYMARKET {
			found = true
			break
		}
	}
	if !found {
		fmt.Println("no polymarket found in configuration, quitting")
		return
	}

	c, err := createSymbolMngr(viper.GetString(env.CONFIG_PATH))
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
		env.REDIS_ADDR,
		env.REDIS_PW,
	})
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
	c, err := createSymbolMngr(viper.GetString(env.CONFIG_PATH))
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}

	err = pythclient.Run(
		c,
		viper.GetString(env.REDIS_ADDR),
		viper.GetString(env.REDIS_PW),
	)
	if err != nil {
		slog.Error(err.Error())
	}
}

func createSymbolMngr(fileName string) (*utils.SymbolManager, error) {
	mngr, err := utils.NewSymbolManager(fileName)
	if err != nil {
		return nil, err
	}
	return mngr, nil
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
