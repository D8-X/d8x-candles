package utils

import (
	"d8x-candles/env"
	"fmt"
	"log/slog"
	"testing"
	"time"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
	"github.com/spf13/viper"
)

func loadEnv() *viper.Viper {
	viper.SetConfigFile("../../.env")
	if err := viper.ReadInConfig(); err != nil {
		slog.Error("could not load .env file" + err.Error())
	}
	return viper.GetViper()
}

func TestDec2Hex(t *testing.T) {
	tokenId := "21742633143463906290569050155826241533067272736897614950488156847949938836455"
	idHex, err := Dec2Hex(tokenId)
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}
	fmt.Println("hex id =", idHex)
	idDec2, err := Hex2Dec(idHex)
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}
	if idDec2 != tokenId {
		fmt.Println("no match")
		t.FailNow()
	}

}

func TestRedisGetFirstTimestamp(t *testing.T) {
	v := loadEnv()
	// ruedis client
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{v.GetString(env.REDIS_ADDR)}, Password: v.GetString(env.REDIS_PW)})
	if err != nil {
		fmt.Printf("error %v", err)
		t.FailNow()
	}
	ts := RedisGetFirstTimestamp(&client, d8xUtils.PXTYPE_PYTH, "BTC-USD")
	fmt.Println(ts)
	tsNow := time.Now().UnixMilli()
	diff := (tsNow - ts) / 1000
	fmt.Printf("age in seconds = %d\n", diff)
}
