package utils

import (
	"testing"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
)

func TestCcyAvailability(t *testing.T) {
	v := loadEnv()
	REDIS_ADDR := v.GetString("REDIS_ADDR")
	REDIS_PW := v.GetString("REDIS_PW")
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{REDIS_ADDR}, Password: REDIS_PW})
	if err != nil {
		t.FailNow()
	}
	err = RedisSetCcyAvailable(&client, d8xUtils.PXTYPE_PYTH, []string{"ETH", "BTC", "USDC"})
	if err != nil {
		t.FailNow()
	}
	avail, err := RedisAreCcyAvailable(&client, d8xUtils.PXTYPE_PYTH, []string{"BTC", "ETH", "USD"})
	if err != nil {
		t.FailNow()
	}
	expected := []bool{true, true, false}
	for j := range expected {
		if expected[j] != avail[j] {
			t.FailNow()
		}
	}
}
