package utils

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
)

type DataPoint struct {
	Timestamp int64
	Value     float64
}

type PriceObservations struct {
	T []uint32  `json:"t"` // time (ts seconds, start)
	P []float64 `json:"p"` // price
}

type PriceUpdateResponse struct {
	Type      string `json:"type"`
	PriceFeed struct {
		ID    string `json:"id"`
		Price struct {
			Price       string `json:"price"`
			Conf        string `json:"conf"`
			Expo        int    `json:"expo"`
			PublishTime int64  `json:"publish_time"`
		} `json:"price"`
		EMAPrice struct {
			Price       string `json:"price"`
			Conf        string `json:"conf"`
			Expo        int    `json:"expo"`
			PublishTime int64  `json:"publish_time"`
		} `json:"ema_price"`
	} `json:"price_feed"`
}

type PythStreamData struct {
	Binary PythStreamBinaryData   `json:"binary"`
	Parsed []PythStreamParsedData `json:"parsed"`
}

type PythStreamBinaryData struct {
	Encoding string   `json:"encoding"`
	Data     []string `json:"data"`
}

type PythStreamParsedData struct {
	Id       string    `json:"id"`
	Price    PriceData `json:"price"`
	EMAPrice PriceData `json:"ema_price"`
	Metadata Metadata  `json:"metadata"`
}

type PriceData struct {
	Price       string `json:"price"`
	Conf        string `json:"conf"`
	Expo        int    `json:"expo"`
	PublishTime int64  `json:"publish_time"`
}

// calculate floating point price from 'price' and 'expo'
func (px *PriceData) CalcPrice() float64 {
	x, err := strconv.Atoi(px.Price)
	if err != nil {
		slog.Error("onPriceUpdate error" + err.Error())
		return 0
	}
	pw := px.Expo
	return float64(x) * math.Pow10(pw)
}

type Metadata struct {
	Slot               int   `json:"slot"`
	ProofAvailableTime int64 `json:"proof_available_time"`
	PrevPublishTime    int64 `json:"prev_publish_time"`
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
func (s *SymbolPyth) New(symbol, id, ourSymbol string) error {
	parts := strings.Split(symbol, ".")
	if len(parts) != 2 {
		return fmt.Errorf("symbol must contain '.'. E.g. Crypto.ETH/USD")
	}
	parts[0] = strings.ToLower(parts[0])
	switch parts[0] {
	case `crypto`, `equity`, `fx`, `metal`, `rates`:
	default:
		return fmt.Errorf("invalid asset type. Possible values are `crypto`, `equity`, `fx`, `metal`, `rates`")
	}

	s.AssetType = strings.ToLower(parts[0])
	parts2 := strings.Split(parts[1], "/")
	if len(parts2) != 2 {
		return fmt.Errorf("symbol must contain '/'. E.g. Crypto.ETH/USD")
	}
	s.Symbol = ourSymbol
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

type ConfigFile struct {
	PythAPIEndpoint       string   `json:"pythAPIEndpoint"`
	PythPriceEndpoints    []string `json:"priceServiceHTTPSEndpoints"`
	PredMktPriceEndpoints []string `json:"predMktPriceEndpoints"`
	ObsoleteWS            []string `json:"priceServiceWSEndpoints"`
}
