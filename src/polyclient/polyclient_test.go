package polyclient

import (
	"fmt"
	"testing"
	"time"
)

func TestGetMarketInfo(t *testing.T) {
	id := "0xdd22472e552920b8438158ea7238bfadfa4f736aa4cee91a6b86c39ead110917"
	m, err := GetMarketInfo(id)
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}
	fmt.Println(m)
}

func TestStreamWs(t *testing.T) {
	ids := []string{"21742633143463906290569050155826241533067272736897614950488156847949938836455",
		"48331043336612883890938759509493159234755048973500640148014422747788308965732"}
	pc := NewPolyClient()
	stopCh := make(chan struct{})
	go pc.RunWs(stopCh)
	fmt.Println("adding asset 1")
	err := pc.SubscribeAssetIds([]string{ids[1]})
	if err != nil {
		fmt.Println(err.Error())
		t.FailNow()
	}
	time.Sleep(10 * time.Second)
	fmt.Println("adding asset 2")
	err = pc.SubscribeAssetIds([]string{ids[0]})
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
