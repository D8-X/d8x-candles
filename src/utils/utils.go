package utils

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	"github.com/redis/rueidis"
)

func Dec2Hex(num string) (string, error) {
	number := new(big.Int)
	number, ok := number.SetString(num, 10)
	if !ok {
		return "", fmt.Errorf("converting number to BIG int")
	}

	return "0x" + number.Text(16), nil
}

func Hex2Dec(num string) (string, error) {
	number := new(big.Int)
	num = strings.TrimPrefix(num, "0x")
	number, ok := number.SetString(num, 16)
	if !ok {
		return "", fmt.Errorf("converting number to BIG int")
	}

	return number.Text(10), nil
}

// RedisReCreateTimeSeries destroys existing timeseries and re-creates it
func RedisReCreateTimeSeries(client *rueidis.Client, sym string) error {
	ctx := context.Background()
	c := *client
	slog.Info("create " + sym)
	_, err := c.Do(ctx, c.B().
		TsInfo().Key(sym).Build()).AsMap()
	if err == nil {
		// key exists, we purge the timeseries
		if err = c.Do(ctx, c.B().Del().
			Key(sym).Build()).Error(); err != nil {
			slog.Error("RedisReCreateTimeSeries, failed deleting time series " + sym + ":" + err.Error())
		}
	}
	// key does not exist, create series
	cmd := c.B().TsCreate().Key(sym).
		Retention(86400000 / 2 * 365). // Keep data for 6 months (in milliseconds)
		DuplicatePolicyLast().
		Build()
	err = c.Do(ctx, cmd).Error()
	if err != nil {
		return fmt.Errorf("could not create time-series %v", err)
	}
	return nil
}

// RedisCreateIfNotExistsTs creates a time-series for the given symbol
func RedisCreateIfNotExistsTs(rClient *rueidis.Client, symbol string) error {
	ctx := context.Background()
	key := symbol
	client := *rClient
	exists, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsBool()
	if err != nil {
		return fmt.Errorf("redis query failed %v", err)
	}
	if !exists {
		// add timeseries
		slog.Info("adding time series", "symbol", symbol)
		cmd := client.B().TsCreate().Key(key).
			Retention(86400000 * 365 / 2). // Keep data for 6 months (in milliseconds)
			DuplicatePolicyLast().
			Build()
		err = client.Do(ctx, cmd).Error()
		if err != nil {
			return fmt.Errorf("could not create time-series %v", err)
		}
	}
	return nil
}

func RedisAddPriceObs(client *rueidis.Client, sym string, price float64, timestampMs int64) error {
	ctx := context.Background()
	c := *client
	ts := strconv.FormatInt(timestampMs, 10)
	resp := c.Do(ctx,
		c.B().TsAdd().Key(sym).Timestamp(ts).Value(price).Build())
	if resp.Error() != nil {
		return fmt.Errorf("RedisAddPriceObs " + sym + ": " + resp.Error().Error())
	}
	return nil
}

func RedisGetFirstTimestamp(client *rueidis.Client, sym string) int64 {
	ctx := context.Background()
	c := *client
	cmd := c.B().TsRange().Key(sym).Fromtimestamp("0").Totimestamp("+").Count(1).Build()
	result, err := c.Do(ctx, cmd).ToArray()
	if err != nil || len(result) == 0 {
		return 0
	}
	// Extract timestamp from the first element
	tsValue, err := result[0].ToArray()
	if err != nil {
		return 0
	}
	timestamp, err := tsValue[0].AsInt64()
	if err != nil {
		return 0
	}
	return timestamp
}

// RedisCalcTriangPrice calculates the triangulated price and returns it, plus the timestamp
// of the oldest price involved
func RedisCalcTriangPrice(redisClient *rueidis.Client, triang d8x_futures.Triangulation) (float64, int64, error) {
	client := *redisClient
	ctx := context.Background()
	var px float64 = 1
	tsOldest := time.Now().UnixMilli()
	for j, sym := range triang.Symbol {
		key := sym
		cmd := client.B().TsGet().Key(key).Build()
		res, err := client.Do(ctx, cmd).ToArray()
		if err != nil {
			return 0, 0, fmt.Errorf("price update failed %v", err)
		}
		price, err := res[1].ToFloat64()
		if err != nil {
			return 0, 0, fmt.Errorf("price update failed %v", err)
		}
		ts, err := res[0].ToInt64()
		if err != nil {
			return 0, 0, fmt.Errorf("price update failed %v", err)
		}
		if ts < tsOldest {
			tsOldest = ts
		}
		if triang.IsInverse[j] {
			px = px * 1 / price
		} else {
			px = px * price
		}
	}
	return px, tsOldest, nil
}

func RedisPublishPriceChange(redisClient *rueidis.Client, symbols string) error {
	c := *redisClient
	return c.Do(context.Background(),
		c.B().Publish().Channel(PRICE_UPDATE_MSG).Message(symbols).Build()).Error()
}

type MarketHours struct {
	IsOpen    bool  `json:"is_open"`
	NextOpen  int64 `json:"next_open"`
	NextClose int64 `json:"next_close"`
}

type MarketInfo struct {
	MarketHours MarketHours
	AssetType   string `json:"assetType"`
}

func GetMarketInfo(ctx context.Context, client *rueidis.Client, ticker string) (MarketInfo, error) {
	c := *client
	hm, err := c.Do(ctx, c.B().Hgetall().Key(ticker+":mkt_info").Build()).AsStrMap()
	if err != nil {
		return MarketInfo{}, err
	}
	if len(hm) == 0 {
		return MarketInfo{}, errors.New("ticker not found")
	}
	isOpen, _ := strconv.ParseBool(hm["is_open"])
	nxtOpen, _ := strconv.ParseInt(hm["nxt_open"], 10, 64)
	nxtClose, _ := strconv.ParseInt(hm["nxt_close"], 10, 64)
	asset := hm["asset_type"]
	// determine market open/close based on current timestamp and
	// next close ts (can be outdated as long as not outdated for more than
	// closing period)
	now := time.Now().UTC().Unix()
	var isClosed bool
	if hm["asset_type"] == POLYMARKET_TYPE {
		// we cannot rely on nxtOpen and nxtClose
		isClosed = !isOpen
	} else {
		isClosed = nxtClose != 0 &&
			((!isOpen && now < nxtOpen) ||
				(isOpen && now > nxtClose))
	}

	var mh = MarketHours{
		IsOpen:    !isClosed,
		NextOpen:  nxtOpen,
		NextClose: nxtClose,
	}
	var m = MarketInfo{MarketHours: mh, AssetType: asset}
	return m, nil
}

func SetMarketHours(rc *rueidis.Client, sym string, mh MarketHours, assetType string) error {
	ctx := context.Background()

	assetType = strings.ToLower(assetType)
	c := *rc
	var nxto, nxtc string

	nxto = strconv.FormatInt(mh.NextOpen, 10)
	nxtc = strconv.FormatInt(mh.NextClose, 10)

	c.Do(ctx, c.B().Hset().Key(sym+":mkt_info").
		FieldValue().FieldValue("is_open", strconv.FormatBool(mh.IsOpen)).
		FieldValue("nxt_open", nxto).
		FieldValue("nxt_close", nxtc).
		FieldValue("asset_type", assetType).Build())
	return nil
}
