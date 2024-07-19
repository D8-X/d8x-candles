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

func CreateRedisTimeSeries(client *RueidisClient, sym string) {
	slog.Info("create " + sym)
	_, err := (*client.Client).Do(client.Ctx, (*client.Client).B().
		TsInfo().Key(sym).Build()).AsMap()
	if err == nil {
		// key exists, we purge the timeseries
		if err = (*client.Client).Do(client.Ctx, (*client.Client).B().Del().
			Key(sym).Build()).Error(); err != nil {
			slog.Error("CreateRedisTimeSeries, failed deleting time series " + sym + ":" + err.Error())
		}
	}
	// key does not exist, create series
	(*client.Client).Do(client.Ctx, (*client.Client).B().TsCreate().Key(sym).DuplicatePolicyLast().Build())
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
	isClosed := nxtClose != 0 &&
		((!isOpen && now < nxtOpen) ||
			(isOpen && now > nxtClose))
	var mh = MarketHours{
		IsOpen:    !isClosed,
		NextOpen:  nxtOpen,
		NextClose: nxtClose,
	}
	var m = MarketInfo{MarketHours: mh, AssetType: asset}
	return m, nil
}

func SetMarketHours(rc *RueidisClient, sym string, mh MarketHours, assetType string) error {
	assetType = strings.ToLower(assetType)
	c := *rc.Client
	var nxto, nxtc string

	nxto = strconv.FormatInt(mh.NextOpen, 10)
	nxtc = strconv.FormatInt(mh.NextClose, 10)

	c.Do(rc.Ctx, c.B().Hset().Key(sym+":mkt_info").
		FieldValue().FieldValue("is_open", strconv.FormatBool(mh.IsOpen)).
		FieldValue("nxt_open", nxto).
		FieldValue("nxt_close", nxtc).
		FieldValue("asset_type", assetType).Build())
	return nil
}
