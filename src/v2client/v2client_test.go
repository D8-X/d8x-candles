package v2client

import (
	"d8x-candles/env"
	"fmt"
	"log/slog"
	"testing"

	"github.com/spf13/viper"
)

func loadEnv() *viper.Viper {
	viper.SetConfigFile("../../.env")
	if err := viper.ReadInConfig(); err != nil {
		slog.Error("could not load .env file" + err.Error())
	}
	return viper.GetViper()
}

func TestV2Client(t *testing.T) {
	v := loadEnv()
	cRpc := "../../config/rpc_conf.json"
	conf := "../../config/v2_idx_conf.json"
	v3, err := NewV2Client(cRpc, v.GetString(env.REDIS_ADDR), v.GetString(env.REDIS_PW), v.GetInt(env.CHAIN_ID), conf)
	if err != nil {
		fmt.Printf("error %v", err)
		t.FailNow()
	}
	v3.Filter()
	fmt.Print("done")
}
