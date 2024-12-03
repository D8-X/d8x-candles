package utils

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
	"golang.org/x/exp/slog"
)

// REDIS set name
const RDS_AVAIL_TICKER_SET string = "avail" // :d8xUtils.PriceType
const RDS_AVAIL_CCY_SET string = "avail_ccy"
const RDS_TICKER_REQUEST = "request"
const RDS_PRICE_UPDATE_MSG = "px_update"

type Aggr int

const (
	AGGR_MIN Aggr = iota
	AGGR_MAX
	AGGR_FIRST
	AGGR_LAST
	AGGR_NONE
)

// RedisCreateIfNotExistsTs creates a time-series for the given symbol
func RedisCreateIfNotExistsTs(rClient *rueidis.Client, pxtype d8xUtils.PriceType, symbol string) error {
	ctx := context.Background()
	key := pxtype.String() + ":" + symbol
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

func RedisAddPriceObs(client *rueidis.Client, pxtype d8xUtils.PriceType, sym string, price float64, timestampMs int64) error {
	ctx := context.Background()
	c := *client
	ts := strconv.FormatInt(timestampMs, 10)
	key := pxtype.String() + ":" + sym
	resp := c.Do(ctx,
		c.B().TsAdd().Key(key).Timestamp(ts).Value(price).Build())
	if resp.Error() != nil {
		return fmt.Errorf("RedisAddPriceObs " + sym + ": " + resp.Error().Error())
	}
	return nil
}

// RedisDelPrefix deletes all keys with the given prefix, except the given keys
func RedisDelPrefix(client *rueidis.Client, prfx string, exKeys []string) error {
	ctx := context.Background()
	c := *client
	var cursor uint64
	for {
		resp := c.Do(ctx, c.B().Scan().Cursor(cursor).Match(prfx).Count(10).Build())
		if resp.Error() != nil {
			return fmt.Errorf("RedisDelPrefix SCAN: %v", resp.Error())
		}
		scanResult, err := resp.AsScanEntry()
		if err != nil {
			return fmt.Errorf("RedisDelPrefix SCAN response: %v", err)
		}
		cursor = scanResult.Cursor
		keys := scanResult.Elements
		todel := make([]string, 0, len(keys))
		for _, ky := range keys {
			if slices.Contains(exKeys, ky) {
				continue
			}
			fmt.Printf("removing %s from redis\n", ky)
			todel = append(todel, ky)
		}
		if len(todel) == 0 {
			if cursor == 0 {
				return nil
			}
			continue
		}
		err = c.Do(ctx, c.B().Del().Key(todel...).Build()).Error()
		if err != nil {
			return fmt.Errorf("RedisDelPrefix DEL response: %v", err)
		}
		// If the cursor is 0, the iteration is complete
		if cursor == 0 {
			return nil
		}
	}
}

// sym of the form ETH-USD
func PricesToRedis(client *rueidis.Client, sym string, pxtype d8xUtils.PriceType, obs PriceObservations) error {
	err := RedisReCreateTimeSeries(client, pxtype, sym)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for k := 0; k < len(obs.P); k++ {
		// store prices in ms
		val := obs.P[k]
		t := int64(obs.T[k]) * 1000
		wg.Add(1)
		go func(sym string, t int64, val float64) {
			defer wg.Done()
			RedisAddPriceObs(client, pxtype, sym, val, t)
		}(sym, t, val)
	}
	wg.Wait()
	// set the symbol as available
	c := *client
	key := RDS_AVAIL_TICKER_SET + ":" + pxtype.String()
	c.Do(context.Background(), c.B().Sadd().Key(key).Member(sym).Build())
	return nil
}

func RedisCleanAfter(client *rueidis.Client, pxtype d8xUtils.PriceType, sym string, tsMs int64) error {
	ctx := context.Background()
	c := *client
	key := pxtype.String() + ":" + sym
	now := time.Now().UnixMilli()
	cmd := c.B().TsDel().Key(key).FromTimestamp(tsMs).ToTimestamp(now).Build()
	return c.Do(ctx, cmd).Error()
}

func RedisGetFirstTimestamp(client *rueidis.Client, pxtype d8xUtils.PriceType, sym string) int64 {
	ctx := context.Background()
	c := *client
	key := pxtype.String() + ":" + sym
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
func RedisCalcTriangPrice(
	redisClient *rueidis.Client,
	pxtype d8xUtils.PriceType,
	triang d8x_futures.Triangulation,
) (float64, int64, error) {

	client := *redisClient
	ctx := context.Background()
	var px float64 = 1
	tsOldest := time.Now().UnixMilli()
	for j, sym := range triang.Symbol {
		key := pxtype.String() + ":" + sym
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
		c.B().Publish().Channel(RDS_PRICE_UPDATE_MSG).Message(symbols).Build()).Error()
}

// RedisReCreateTimeSeries destroys existing timeseries and re-creates it
func RedisReCreateTimeSeries(client *rueidis.Client, pxtype d8xUtils.PriceType, sym string) error {
	ctx := context.Background()
	c := *client
	key := pxtype.String() + ":" + sym
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

// RedisSetCcyAvailable sets currencies available, e.g., ccys=[]string{"USDC", "BTC", ...}
func RedisSetCcyAvailable(client *rueidis.Client, pxtype d8xUtils.PriceType, ccys []string) error {
	ctx := context.Background()
	c := *client
	setKey := RDS_AVAIL_CCY_SET + ":" + pxtype.String()
	// Delete the entire set
	cmds := c.B().Del().Key(setKey).Build()
	if res := c.Do(ctx, cmds); res.Error() == nil {
		slog.Info("redis: deleted existing " + setKey)
	}
	slog.Info("create " + setKey)
	cmd := c.B().Sadd().Key(setKey).Member(ccys...).Build()
	res := c.Do(ctx, cmd)
	return res.Error()
}

// RedisAreCcyAvailable checks which of the provided currencies are available in the AVAIL_CCY_SET
// and returns a boolean array corresponding to ccys
func RedisAreCcyAvailable(client *rueidis.Client, pxtype d8xUtils.PriceType, ccys []string) ([]bool, error) {
	ctx := context.Background()
	c := *client
	setKey := RDS_AVAIL_CCY_SET + ":" + pxtype.String()
	cmd := c.B().Smembers().Key(setKey).Build()
	availCcys, err := c.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return nil, err
	}
	avail := make([]bool, len(ccys))
	for k, c0 := range ccys {
		for _, c1 := range availCcys {
			if c0 == c1 {
				avail[k] = true
				break
			}
		}
	}
	return avail, nil
}

func RedisIsSymbolAvailable(client *rueidis.Client, pxtype d8xUtils.PriceType, sym string) bool {
	ctx := context.Background()
	key := RDS_AVAIL_TICKER_SET + ":" + pxtype.String()
	c := *client
	cmd := c.B().Sismember().Key(key).Member(sym).Build()
	isMember, err := c.Do(ctx, cmd).AsBool()
	if err != nil {
		slog.Error("IsSymbolAvailable " + sym + "error:" + err.Error())
		return false
	}
	return isMember
}

func RedisSetMarketHours(rc *rueidis.Client, sym string, mh MarketHours, assetType d8xUtils.AssetClass) error {
	ctx := context.Background()

	c := *rc
	var nxto, nxtc string

	nxto = strconv.FormatInt(mh.NextOpen, 10)
	nxtc = strconv.FormatInt(mh.NextClose, 10)

	c.Do(ctx, c.B().Hset().Key(sym+":mkt_info").
		FieldValue().FieldValue("is_open", strconv.FormatBool(mh.IsOpen)).
		FieldValue("nxt_open", nxto).
		FieldValue("nxt_close", nxtc).
		FieldValue("asset_type", assetType.String()).Build())
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
	asset := d8xUtils.AssetClassMap[hm["asset_type"]]
	// determine market open/close based on current timestamp and
	// next close ts (can be outdated as long as not outdated for more than
	// closing period)
	now := time.Now().UTC().Unix()
	var isClosed bool
	if asset == d8xUtils.ACLASS_POLYMKT {
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

func RedisTsGet(client *rueidis.Client, sym string, pxtype d8xUtils.PriceType) (DataPoint, error) {
	key := pxtype.String() + ":" + sym
	vlast, err := (*client).Do(context.Background(), (*client).B().TsGet().Key(key).Build()).ToArray()
	if err != nil {
		return DataPoint{}, err
	}
	if len(vlast) < 2 {
		return DataPoint{}, errors.New("Could not find ts for " + key)
	}
	ts, _ := vlast[0].AsInt64()
	v, _ := vlast[1].AsFloat64()
	d := DataPoint{Timestamp: ts, Value: v}
	return d, nil
}

// OhlcFromRedis queries OHLC data from REDIS price cache, timestamps in ms
// sym is of the form btc-usd
func OhlcFromRedis(client *rueidis.Client, sym string, pxtype d8xUtils.PriceType, fromTs int64, toTs int64, resolSec uint32) ([]OhlcData, error) {

	timeBucket := int64(resolSec) * 1000
	//agg.Count = 100
	// collect aggregations
	aggregations := []Aggr{AGGR_FIRST, AGGR_MAX, AGGR_MIN, AGGR_LAST}

	var redisData []*[]DataPoint
	for _, a := range aggregations {
		data, err := RangeAggr(client, sym, pxtype, fromTs, toTs, timeBucket, a)
		if err != nil {
			return []OhlcData{}, err
		}
		redisData = append(redisData, &data)
	}
	// in case some arrays are longer
	K := min(len(*redisData[0]), min(len(*redisData[1]), min(len(*redisData[2]), len(*redisData[3]))))

	// store in candle format
	var ohlc []OhlcData
	var tOld int64 = 0
	for k := 0; k < K; k++ {
		var data OhlcData
		data.TsMs = (*redisData[0])[k].Timestamp
		data.Time = ConvertTimestampToISO8601(data.TsMs)
		data.O = (*redisData[0])[k].Value
		data.H = (*redisData[1])[k].Value
		data.L = (*redisData[2])[k].Value
		data.C = (*redisData[3])[k].Value

		// insert artificial data for gaps before adding 'data'
		numGaps := (data.TsMs - tOld) / timeBucket
		for j := 0; j < int(numGaps)-1 && k > 0; j++ {
			var dataGap OhlcData
			dataGap.TsMs = tOld + int64(j+1)*timeBucket
			dataGap.Time = ConvertTimestampToISO8601(dataGap.TsMs)
			// set all data to close of previous OHLC observation
			dataGap.O = (*redisData[3])[k].Value
			dataGap.H = (*redisData[3])[k].Value
			dataGap.L = (*redisData[3])[k].Value
			dataGap.C = (*redisData[3])[k].Value
			ohlc = append(ohlc, dataGap)
		}
		tOld = data.TsMs
		ohlc = append(ohlc, data)
	}
	return ohlc, nil
}

// RangeAggr aggregates the redis prices for the given symbol/price type over the given horizon and bucketDuration according
// to 'aggr'
func RangeAggr(r *rueidis.Client, sym string, pxtype d8xUtils.PriceType, fromTs int64, toTs int64, bucketDur int64, aggr Aggr) ([]DataPoint, error) {
	key := pxtype.String() + ":" + sym
	var cmd rueidis.Completed
	fromTs = int64(fromTs/bucketDur) * bucketDur
	switch aggr {
	case AGGR_MIN:
		cmd = (*r).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationMin().Bucketduration(bucketDur).Build()
	case AGGR_MAX:
		cmd = (*r).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationMax().Bucketduration(bucketDur).Build()
	case AGGR_FIRST:
		cmd = (*r).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationFirst().Bucketduration(bucketDur).Build()
	case AGGR_LAST:
		cmd = (*r).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationLast().Bucketduration(bucketDur).Build()
	case AGGR_NONE: //no aggregation
		cmd = (*r).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			Build()
	default:
		return []DataPoint{}, errors.New("invalid aggr type")
	}
	raw, err := (*r).Do(context.Background(), cmd).ToAny()
	if err != nil {
		return []DataPoint{}, err
	}
	data := ParseTsRange(raw)

	return data, nil
}
