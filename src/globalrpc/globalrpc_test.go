package globalrpc

import (
	"d8x-candles/env"
	"fmt"
	"log/slog"
	"testing"
	"time"

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

func TestUrlToRedis(t *testing.T) {
	v := loadEnv()
	client, err := rueidis.NewClient(
		rueidis.ClientOption{
			InitAddress: []string{v.GetString(env.REDIS_ADDR)},
			Password:    v.GetString(env.REDIS_PW)})
	if err != nil {
		t.FailNow()
	}
	urls := []string{
		"https://bera-testnet.nodeinfra.com",
		"https://bartio.rpc.berachain.com",
		"https://bartio.drpc.org"}
	err = urlToRedis(42, TypeHTTPS, urls, &client)
	if err != nil {
		fmt.Printf("failed to insert urls: %v", err)
		t.FailNow()
	}
	// now try again (should not have an effect)
	err = urlToRedis(42, TypeHTTPS, urls, &client)
	if err != nil {
		fmt.Printf("failed to insert urls in second attempt: %v", err)
		t.FailNow()
	}

}

func TestReturnLock(t *testing.T) {
	v := loadEnv()
	gr, err := NewGlobalRpc("../../config/v3_rpc_conf.json", v.GetString(env.REDIS_ADDR), v.GetString(env.REDIS_PW))
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}

	numRpc := len(gr.Config.Https)
	N := numRpc + 2
	receipts := make([]Receipt, N)
	for k := 0; k < N; k++ {
		receipts[k], err = gr.GetAndLockRpc(TypeHTTPS, 10)
		if err != nil {
			fmt.Println(err.Error())
			t.FailNow()
		}
		if k >= numRpc-1 {
			fmt.Println("returning " + receipts[k-2].Url)
			gr.ReturnLock(receipts[k-2])
		}
		fmt.Printf("got URL %s\n", receipts[k].Url)
	}
	time.Sleep(5 * time.Second)
	for k := 0; k < N; k++ {
		gr.ReturnLock(receipts[k])
	}
	fmt.Println("done")
}
