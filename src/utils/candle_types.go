package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type DataPoints struct {
	TimestampMs []int64
	Value       []float64
}

type SymbolPyth struct {
	AssetType  string
	PythSymbol string
	Symbol     string
	id         string
}

// display the pyth symbol per pyth convention
// E.g., Crypto.BTC/USD
func (s *SymbolPyth) ToString() string {
	return s.PythSymbol
}

func (s *SymbolPyth) PairString() string {
	return s.Symbol
}

// Create a new Pyth symbol from a string such as Crypto.ETH/USD
// and the id
func (s *SymbolPyth) New(symbol string, id string) error {
	parts := strings.Split(symbol, ".")
	if len(parts) != 2 {
		return fmt.Errorf("Symbol must contain '.'. E.g. Crypto.ETH/USD")
	}
	parts[0] = strings.ToLower(parts[0])
	switch parts[0] {
	case `crypto`, `equity`, `fx`, `metal`, `rates`:
	default:
		return fmt.Errorf("Invalid asset type. Possible values are `crypto`, `equity`, `fx`, `metal`, `rates`.")
	}

	s.AssetType = strings.ToLower(parts[0])
	parts2 := strings.Split(parts[1], "/")
	if len(parts2) != 2 {
		return fmt.Errorf("Symbol must contain '/'. E.g. Crypto.ETH/USD")
	}
	s.Symbol = strings.ToLower(parts2[0]) + "-" + strings.ToLower(parts2[1])
	s.id = id
	s.PythSymbol = symbol
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
func (c *PythCandleResolution) ToPythString() string {
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

type CandlePeriod struct {
	Name           string
	TimeMs         int
	DisplayRangeMs int
}

type PriceConfig struct {
	ConfigFile           ConfigFile
	PythIdToSym          map[string]string       //pyth id (0xabc..) to symbol (btc-usd)
	SymToDependentTriang map[string][]string     //sym to all dependent triangulations
	SymToTriangPath      map[string][]string     //sym to triangulation path
	CandlePeriodsMs      map[string]CandlePeriod //period 1m,5m,... to timeMs and displayRangeMs
}

type ConfigFile struct {
	PythAPIEndpoint     string `json:"pythAPIEndpoint"`
	PythPriceWSEndpoint string `json:"priceServiceWSEndpoint"`
	PriceFeeds          []struct {
		Symbol     string `json:"symbol"`
		SymbolPyth string `json:"symbolPyth"`
		Id         string `json:"id"`
	} `json:"priceFeeds"`
	Triangulations []struct {
		Target string   `json:"target"`
		Path   []string `json:"path"`
	} `json:"triangulations"`
	SupportedCandlePeriods []struct {
		Period         string `json:"period"`
		TimeMs         int    `json:"timeMs"`
		DisplayRangeMs int    `json:"displayRangeMs"`
	} `json:"supportedCandlePeriods"`
}

func (c *PriceConfig) LoadPriceConfig(fileName string) error {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &c.ConfigFile)
	if err != nil {
		return err
	}
	c.extractPythIdToSymbolMap()
	c.extractSymbolToTriangTarget()
	c.extractTriangulationMap()
	c.extractCandlePeriods()
	return nil
}

// creates a map from ids "0x32121..." to symbols "xau-usd"
func (c *PriceConfig) extractPythIdToSymbolMap() {
	m := make(map[string]string, len(c.ConfigFile.PriceFeeds))
	for _, el := range c.ConfigFile.PriceFeeds {
		idTrim, _ := strings.CutPrefix(el.Id, "0x")
		m[idTrim] = el.Symbol
	}
	c.PythIdToSym = m
}

// From the data of the form { "target": "btc-usdc", "path": ["*", "btc-usd", "/", "usdc-usd"] },
// we create a mapping of the underlying symbols to the target.
// This is to quickly find all affected triangulations on a price change
func (c *PriceConfig) extractSymbolToTriangTarget() {
	m := make(map[string][]string)
	for k := 0; k < len(c.ConfigFile.Triangulations); k++ {
		path := c.ConfigFile.Triangulations[k].Path
		for j := 1; j < len(path); j = j + 2 {
			m[path[j]] = append(m[path[j]], c.ConfigFile.Triangulations[k].Target)
		}
	}
	c.SymToDependentTriang = m
}

// get map for { "target": "btc-usdc", "path": ["*", "btc-usd", "/", "usdc-usd"] }
// from target -> path (map[string][]string)
func (c *PriceConfig) extractTriangulationMap() {
	m := make(map[string][]string)
	for k := 0; k < len(c.ConfigFile.Triangulations); k++ {
		t := c.ConfigFile.Triangulations[k].Target
		m[t] = c.ConfigFile.Triangulations[k].Path
	}
	c.SymToTriangPath = m
}

func (c *PriceConfig) extractCandlePeriods() {
	c.CandlePeriodsMs = make(map[string]CandlePeriod, len(c.ConfigFile.SupportedCandlePeriods))
	for _, el := range c.ConfigFile.SupportedCandlePeriods {
		c.CandlePeriodsMs[el.Period] = CandlePeriod{Name: el.Period, TimeMs: el.TimeMs, DisplayRangeMs: el.DisplayRangeMs}
	}
}

func (c *PriceConfig) IsSymbolAvailable(sym string) bool {
	// symbol is a triangulated symbol?
	s := c.SymToTriangPath[sym]
	if len(s) > 0 {
		return true
	}
	// symbol is part of a triangulation?
	t := c.SymToTriangPath[sym]
	if len(t) > 0 {
		return true
	}
	// symbol is not used in triangulations but available
	for _, el := range c.ConfigFile.PriceFeeds {
		if el.Symbol == sym {
			return true
		}
	}
	return false
}
