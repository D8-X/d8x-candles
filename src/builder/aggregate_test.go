package builder

import (
	"fmt"
	"testing"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
)

func TestNewRedisClient(t *testing.T) {
	host := "localhost:6379"
	//password := ""
	var client = redistimeseries.NewClient(host, "client", nil)
	var keyname = "eth-usd"
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
	client.Add(keyname, 2, 31.0)
	client.Add(keyname, 3, 32.0)
	client.Add(keyname, 4, 33.0)
	client.Add(keyname, 5, 34.0)
	client.Add(keyname, 6, 35.0)
	client.Add(keyname, 7, 36.0)
	client.Add(keyname, 8, 37.0)
	client.Add(keyname, 9, 36.5)
	client.Add(keyname, 10, 55.5)
	client.Add(keyname, 11, 66.5)
	client.Add(keyname, 12, 11.5)
	// ! aggregation starts at timestamp zero
	agg := redistimeseries.DefaultRangeOptions
	agg.AggType = redistimeseries.MaxAggregation
	agg.TimeBucket = 2
	datapoints, err := client.RangeWithOptions(keyname, 1, 12, agg)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%v\n", datapoints)
	// Retrieve the latest data point
	//	fmt.Printf("Latest datapoint: timestamp=%d value=%f\n", latestDatapoint.Timestamp, latestDatapoint.Value)
}
