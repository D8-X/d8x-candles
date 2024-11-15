package utils

import (
	"fmt"
	"math/big"
	"strings"
)

type PriceType int

const (
	TYPE_POLYMARKET PriceType = iota
	TYPE_PYTH
	TYPE_V2
	TYPE_V3
	TYPE_UNKNOWN
)

// Create a slice with all possible PriceType values
var PriceTypes = []PriceType{
	TYPE_POLYMARKET,
	TYPE_PYTH,
	TYPE_V2,
	TYPE_V3,
}

// PriceType to string. needs to align with
// priceFeedConfig on https://github.com/D8-X/sync-hub
func (p PriceType) ToString() string {
	if p == TYPE_POLYMARKET {
		return "polymarket"
	}
	if p == TYPE_PYTH {
		return "pyth"
	}
	if p == TYPE_V2 {
		return "univ2"
	}
	if p == TYPE_V3 {
		return "univ3"
	}
	return "unknown"
}

type OhlcData struct {
	TsMs int64   `json:"start"` // start time in milliseconds
	Time string  `json:"time"`  //e.g. "2023-07-18T15:00:00.000Z"
	O    float64 `json:"open"`
	H    float64 `json:"high"`
	L    float64 `json:"low"`
	C    float64 `json:"close"`
}

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
