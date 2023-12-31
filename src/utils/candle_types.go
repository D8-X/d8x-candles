package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"log/slog"

	"github.com/redis/rueidis"
)

type DataPoint struct {
	Timestamp int64
	Value     float64
}

type RueidisClient struct {
	Client *rueidis.Client
	Ctx    context.Context
}

func (r *RueidisClient) Get(key string) (DataPoint, error) {
	vlast, err := (*r.Client).Do(r.Ctx, (*r.Client).B().TsGet().Key(key).Build()).ToArray()
	if err != nil {
		return DataPoint{}, err
	}
	ts, _ := vlast[0].AsInt64()
	v, _ := vlast[1].AsFloat64()
	d := DataPoint{Timestamp: ts, Value: v}
	return d, nil
}

func (r *RueidisClient) RangeAggr(key string, fromTs int64, toTs int64, bucketDur int64, aggr string) ([]DataPoint, error) {
	var cmd rueidis.Completed
	fromTs = int64(fromTs/bucketDur) * bucketDur
	switch aggr {
	case "min":
		cmd = (*r.Client).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationMin().Bucketduration(bucketDur).Build()
	case "max":
		cmd = (*r.Client).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationMax().Bucketduration(bucketDur).Build()
	case "first":
		cmd = (*r.Client).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationFirst().Bucketduration(bucketDur).Build()
	case "last":
		cmd = (*r.Client).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			AggregationLast().Bucketduration(bucketDur).Build()
	case "": //no aggregation
		cmd = (*r.Client).B().TsRange().Key(key).
			Fromtimestamp(strconv.FormatInt(fromTs, 10)).Totimestamp(strconv.FormatInt(toTs, 10)).
			Align("-").
			Build()
	default:
		return []DataPoint{}, errors.New("Invalid aggr type")
	}
	raw, err := (*r.Client).Do(r.Ctx, cmd).ToAny()
	if err != nil {
		return []DataPoint{}, err
	}
	data := ParseTsRange(raw)

	return data, nil
}

func ParseTsRange(data interface{}) []DataPoint {
	rSlice, ok := data.([]interface{})
	if !ok {
		return []DataPoint{}
	}

	dataPoints := make([]DataPoint, len(rSlice))
	for k, innerSlice := range rSlice {
		if inner, ok := innerSlice.([]interface{}); ok && len(inner) == 2 {
			if intValue, ok := inner[0].(int64); ok {
				dataPoints[k].Timestamp = intValue
			}
			if floatValue, ok := inner[1].(float64); ok {
				dataPoints[k].Value = floatValue
			}
		}
	}
	return dataPoints
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
	PythAPIEndpoint      string   `json:"pythAPIEndpoint"`
	PythPriceWSEndpoints []string `json:"priceServiceWSEndpoints"`
	PriceFeeds           []struct {
		Symbol     string `json:"symbol"`
		SymbolPyth string `json:"symbolPyth"`
		IdVaaTest  string `json:"idVaaTestnet"` // id used for vaa (testnet - otherwise identical to id)
		Id         string `json:"id"`           // id used for benchmarks/price_feeds-endpoint (mainnet)
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

func (c *PriceConfig) LoadPriceConfig(fileName string, network string) error {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &c.ConfigFile)
	if err != nil {
		return err
	}
	c.extractPythIdToSymbolMap(network)
	c.extractSymbolToTriangTarget()
	c.extractTriangulationMap()
	c.extractCandlePeriods()
	return nil
}

// creates a map from ids "0x32121..." to symbols "xau-usd"
func (c *PriceConfig) extractPythIdToSymbolMap(network string) {
	slog.Info("Loading VAA ids for network " + network)
	m := make(map[string]string, len(c.ConfigFile.PriceFeeds))
	for _, el := range c.ConfigFile.PriceFeeds {
		var idTrim string
		if network == "testnet" {
			idTrim, _ = strings.CutPrefix(el.IdVaaTest, "0x")
		} else {
			idTrim, _ = strings.CutPrefix(el.Id, "0x")
		}

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
