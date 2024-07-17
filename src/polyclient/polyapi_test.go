package polyclient

import (
	"d8x-candles/src/utils"
	"fmt"
	"testing"
	"time"
)

func TestGetMarketInfo(t *testing.T) {
	id := "0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917"
	bucket := utils.NewTokenBucket(4, 4.0)
	m, err := GetMarketInfo(bucket, id)
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}
	fmt.Println(m)
	// spam:
	for k := 0; k < 40; k++ {
		m, err = GetMarketInfo(bucket, id)
		if err != nil {
			fmt.Println(err.Error())
			t.FailNow()
		}
		fmt.Printf("%d: %f\n", k, m.Tokens[0].Price)
	}
}

func TestStreamWs(t *testing.T) {
	ids := []string{"21742633143463906290569050155826241533067272736897614950488156847949938836455",
		"48331043336612883890938759509493159234755048973500640148014422747788308965732"}
	pa := NewPolyApi()
	stopCh := make(chan struct{})
	go pa.RunWs(stopCh, nil)
	fmt.Println("adding asset 1")
	err := pa.SubscribeAssetIds([]string{ids[1]}, []string{"0TRUMP"})
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}
	time.Sleep(10 * time.Second)
	fmt.Println("adding asset 2")
	err = pa.SubscribeAssetIds([]string{ids[0]}, []string{"1TRUMP"})
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}
	time.Sleep(360 * time.Second)
	fmt.Println("closing channel")
	close(stopCh)
	time.Sleep(15 * time.Second)
	fmt.Println("done")
}

func TestRestQueryHistory(t *testing.T) {
	id := "21742633143463906290569050155826241533067272736897614950488156847949938836455"
	res, err := RestQueryHistory(utils.NewTokenBucket(4, 4.0), id)
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}
	fmt.Printf("num obs = %d", len(res))
}
