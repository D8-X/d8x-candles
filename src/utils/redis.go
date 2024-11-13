package utils

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	"github.com/redis/rueidis"
	"golang.org/x/exp/slog"
)

type PriceType int

const (
	TYPE_V2 PriceType = iota
	TYPE_V3
	TYPE_PYTH
	TYPE_POLY
)

func (p PriceType) ToString() string {
	if p == TYPE_V2 {
		return "v2"
	}
	if p == TYPE_V3 {
		return "v3"
	}
	if p == TYPE_PYTH {
		return "pyth"
	}
	if p == TYPE_POLY {
		return "poly"
	}
	return "unknown"
}

// RedisCreateIfNotExistsTs creates a time-series for the given symbol
func RedisCreateIfNotExistsTs(rClient *rueidis.Client, pxtype PriceType, symbol string) error {
	ctx := context.Background()
	key := pxtype.ToString() + ":" + symbol
	client := *rClient
	exists, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsBool()
	if err != nil {
		return fmt.Errorf("redis query failed %v", err)
	}
	if !exists {
		// add timeseries
		// we keep the data for a long time. There can be compactions (e.g. Pyth)
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

func RedisAddPriceObs(client *rueidis.Client, pxtype PriceType, sym string, price float64, timestampMs int64) error {
	ctx := context.Background()
	c := *client
	ts := strconv.FormatInt(timestampMs, 10)
	key := pxtype.ToString() + ":" + sym
	resp := c.Do(ctx,
		c.B().TsAdd().Key(key).Timestamp(ts).Value(price).Build())
	if resp.Error() != nil {
		return fmt.Errorf("RedisAddPriceObs " + sym + ": " + resp.Error().Error())
	}
	return nil
}

func RedisGetFirstTimestamp(client *rueidis.Client, pxtype PriceType, sym string) int64 {
	ctx := context.Background()
	c := *client
	key := pxtype.ToString() + ":" + sym
	cmd := c.B().TsRange().Key(key).Fromtimestamp("0").Totimestamp("+").Count(1).Build()
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
func RedisCalcTriangPrice(redisClient *rueidis.Client, pxtype PriceType, triang d8x_futures.Triangulation) (float64, int64, error) {
	client := *redisClient
	ctx := context.Background()
	var px float64 = 1
	tsOldest := time.Now().UnixMilli()
	for j, sym := range triang.Symbol {
		key := pxtype.ToString() + ":" + sym
		cmd := client.B().TsGet().Key(key).Build()
		res, err := client.Do(ctx, cmd).ToArray()
		if err != nil {
			return 0, 0, fmt.Errorf("price update failed %v", err)
		}
		if len(res) == 0 {
			return 0, 0, fmt.Errorf("price update failed: no obs for %s", sym)
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

// RedisPublishIdxPriceChange broadcasts PRICE_UPDATE_MSG.
// For V2 and V3 prices ensure only indices are published,
// otherwise the trader-backend could
// for example publish an ETH-USDT price from V2 to a regular ETH-USD perp.
// Index prices should never overlap
func RedisPublishIdxPriceChange(redisClient *rueidis.Client, symbols string) error {
	c := *redisClient
	return c.Do(context.Background(),
		c.B().Publish().Channel(PRICE_UPDATE_MSG).Message(symbols).Build()).Error()
}

// RedisReCreateTimeSeries destroys existing timeseries and re-creates it
func RedisReCreateTimeSeries(client *rueidis.Client, pxtype PriceType, sym string) error {
	ctx := context.Background()
	c := *client
	key := pxtype.ToString() + ":" + sym
	slog.Info("create " + key)
	_, err := c.Do(ctx, c.B().
		TsInfo().Key(key).Build()).AsMap()
	if err == nil {
		// key exists, we purge the timeseries
		if err = c.Do(ctx, c.B().Del().
			Key(key).Build()).Error(); err != nil {
			slog.Error("RedisReCreateTimeSeries, failed deleting time series " + key + ":" + err.Error())
		}
	}
	// key does not exist, create series
	cmd := c.B().TsCreate().Key(key).
		Retention(86400000 / 2 * 365). // Keep data for 6 months (in milliseconds)
		DuplicatePolicyLast().
		Build()
	err = c.Do(ctx, cmd).Error()
	if err != nil {
		return fmt.Errorf("could not create time-series %v", err)
	}
	return nil
}

func RedisSetMarketHours(rc *rueidis.Client, sym string, mh MarketHours, assetType string) error {
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

func RedisGetMarketInfo(ctx context.Context, client *rueidis.Client, ticker string) (MarketInfo, error) {
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
