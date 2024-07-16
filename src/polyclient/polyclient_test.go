package polyclient

import (
	"fmt"
	"testing"
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
	ids := []string{"21742633143463906290569050155826241533067272736897614950488156847949938836455"}
	err := StreamWs(ids)
	fmt.Println(err.Error())
}
