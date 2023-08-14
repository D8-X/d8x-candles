package utils

import (
	"fmt"
	"strconv"
	"strings"
)

type SymbolPyth struct {
	AssetType string
	Base      string
	Quote     string
}

// display the pyth symbol per pyth convention
// E.g., Crypto.BTC/USD
func (s *SymbolPyth) ToString() string {
	return s.AssetType + "." + s.Base + "/" + s.Quote
}

// Create a new Pyth symbol from a string such as Crypto.ETH/USD
func (s *SymbolPyth) New(symbol string) error {
	parts := strings.Split(symbol, ".")
	if len(parts) != 2 {
		return fmt.Errorf("Symbol must contain '.'. E.g. Crypto.ETH/USD")
	}
	s.AssetType = parts[0]
	parts2 := strings.Split(parts[1], "/")
	if len(parts2) != 2 {
		return fmt.Errorf("Symbol must contain '/'. E.g. Crypto.ETH/USD")
	}
	s.Base = parts2[0]
	s.Quote = parts2[1]
	return nil
}

/*
		1 -> 1 minute (works for at least 24h)
		2, 5, 15, 30, 60, 120, 240, 360, 720 -> min
	    1D, 1W, 1M
*/
type PythCandleResolutionUnit uint8

const (
	MinuteCandle PythCandleResolutionUnit = iota
	DayCandle
	WeekCandle
	MonthCandle
)

type PythCandleResolution struct {
	Resolution uint16
	Unit       PythCandleResolutionUnit
}

func (c *PythCandleResolution) New(timeNumber uint16, timeUnit PythCandleResolutionUnit) error {
	if timeUnit == MinuteCandle {
		validValues := map[uint16]bool{
			1:   true,
			2:   true,
			5:   true,
			15:  true,
			30:  true,
			60:  true,
			120: true,
			240: true,
			360: true,
			720: true,
		}
		if !validValues[timeNumber] {
			return fmt.Errorf("invalid minute resolution {1,2,5,15,30,60,120,240,360,720}")
		}
	} else if timeNumber != 1 {
		return fmt.Errorf("1 required for Day, Week, Month")
	}
	c.Resolution = timeNumber
	c.Unit = timeUnit
	return nil
}

// Display candle resolution as per Pyth API
func (c *PythCandleResolution) ToString() string {
	switch c.Unit {
	case MinuteCandle:
		sNum := strconv.Itoa(int(c.Resolution))
		return sNum
	case DayCandle:
		return "1D"
	case WeekCandle:
		return "1W"
	default: // MonthCandle:
		return "1M"
	}
}
