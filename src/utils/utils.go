package utils

import (
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
)

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
	AssetType   d8xUtils.AssetClass `json:"assetType"`
}

func ReadFile(filename string) ([]byte, error) {
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()

	// Read the file's contents into a byte slice
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		return nil, fmt.Errorf("Failed to read file: %v", err)
	}
	return byteValue, nil
}
