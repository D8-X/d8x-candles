package builder

import (
	"context"
	"d8x-candles/src/utils"
	"fmt"
	"testing"

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
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{"127.0.0.1:6379"}, Password: "23_*PAejOanJma"})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	r, err := client.Do(ctx, client.B().TsRange().Key("btc-usd").
		Fromtimestamp("1693809900000").Totimestamp("1693896300000").Build()).ToAny()
	//	r.val.values[0].values[0].integer
	//	1693809900000
	// r.val.values[0].values[1].string
	// "2.59800108102e+4"

	fmt.Print(r)

	r, err = client.Do(ctx, client.B().TsRange().Key("btc-usd").
		Fromtimestamp("1693809900000").Totimestamp("1693896300000").
		AggregationMax().Bucketduration(300000).Build()).ToAny()
	fmt.Print(r)
	dp := utils.ParseTsRange(r)
	fmt.Print(dp)
	rc := utils.RueidisClient{Client: &client, Ctx: ctx}
	dp2, err := rc.RangeAggr("btc-usd", 1693809900000, 1693896300000, 300000, "max")
	fmt.Print(dp[0])
	fmt.Print(dp2[0])
	client.Close()
}
