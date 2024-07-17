package utils

import (
	"fmt"
	"log/slog"
	"math/big"
	"strings"
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
