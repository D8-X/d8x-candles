package utils

import (
	"context"
	"d8x-candles/env"
	"fmt"
	"testing"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
)

/*
	func TestNewRedisClient(t *testing.T) {
		host := "localhost:6379"
		password := ""
		var client = redistimeseries.NewClient(host, "client", &password)
		var keyname = "abc-def"
		_, haveit := client.Info(keyname)
		if haveit != nil {
			client.CreateKeyWithOptions(keyname, redistimeseries.DefaultCreateOptions)
		}

		client.Add(keyname, 1, 30.1)
		_, haveit2 := client.Info(keyname)
		if haveit2 != nil {
			t.Error(haveit2)
			return
		}
		for k := 0; k < 500; k++ {
			client.Add(keyname, int64(k*2), float64(k))
		}
		// ! aggregation starts at timestamp zero
		agg := redistimeseries.DefaultRangeOptions
		agg.AggType = redistimeseries.MaxAggregation
		agg.TimeBucket = 10
		datapoints0, err := client.Range(keyname, 0, 1000)
		fmt.Printf("%v\n", datapoints0)
		datapoints, err := client.RangeWithOptions(keyname, 0, 1000, agg)
		if err != nil {
			t.Error(err)
		}
		fmt.Printf("%v\n", datapoints)
		// Retrieve the latest data point
		//	fmt.Printf("Latest datapoint: timestamp=%d value=%f\n", latestDatapoint.Timestamp, latestDatapoint.Value)
	}
*/
func TestRueidis(t *testing.T) {
	v := loadEnv()
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{v.GetString(env.REDIS_ADDR)}, Password: v.GetString(env.REDIS_PW)})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	r, err := client.Do(ctx, client.B().TsRange().Key("btc-usd").
		Fromtimestamp("1693809900000").Totimestamp("1693896300000").Build()).ToAny()
	if err != nil {
		panic(err)
	}
	//	r.val.values[0].values[0].integer
	//	1693809900000
	// r.val.values[0].values[1].string
	// "2.59800108102e+4"

	fmt.Print(r)

	r, err = client.Do(ctx, client.B().TsRange().Key("btc-usd").
		Fromtimestamp("1693809900000").Totimestamp("1693896300000").
		AggregationMax().Bucketduration(300000).Build()).ToAny()
	if err != nil {
		panic(err)
	}
	fmt.Print(r)
	dp := ParseTsRange(r)
	fmt.Print(dp)
	dp2, err := RangeAggr(&client, "btc-usd", d8xUtils.PXTYPE_PYTH, 1693809900000, 1693896300000, 300000)
	if err != nil {
		panic(err)
	}
	fmt.Print(dp[0])
	fmt.Print(dp2[0])
	client.Close()
}

func TestRedisAggr(t *testing.T) {
	v := loadEnv()
	REDIS_ADDR := v.GetString("REDIS_ADDR")
	REDIS_PW := v.GetString("REDIS_PW")
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{REDIS_ADDR}, Password: REDIS_PW})
	if err != nil {
		t.FailNow()
	}
	sym := "rds-tst"
	for k := 0; k < 50; k++ {
		var timestampMs int64 = 1 + int64(k)*1000
		RedisAddPriceObs(&client, d8xUtils.PXTYPE_PYTH, sym, float64(k), timestampMs)
	}
	obs, err := RangeAggr(&client, sym, d8xUtils.PXTYPE_PYTH, 0, 50000, 0)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(obs)

	// 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, ...
	// bucket size 10 will take 10 elements and each of it counts for
	// the aggregation, next bucket is non-overlapping
	// aggregation considers the first timestamp
	// => timestamp...timestamp+bucketSize-1 is the range
	// if aF[0].Value != float64(0) {
	// 	t.Errorf("want 1, got %f", aF[0].Value)
	// }
	// // aggregation is exclusive of the last
	// if aL[0].Value != float64(9) {
	// 	t.Errorf("want 9, got %f", aL[0].Value)
	// }

}
