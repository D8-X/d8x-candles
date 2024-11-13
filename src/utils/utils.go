package utils

import (
	"fmt"
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

type MarketHours struct {
	IsOpen    bool  `json:"is_open"`
	NextOpen  int64 `json:"next_open"`
	NextClose int64 `json:"next_close"`
}

type MarketInfo struct {
	MarketHours MarketHours
	AssetType   string `json:"assetType"`
}
