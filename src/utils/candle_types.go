package utils

import (
	"context"
	"d8x-candles/config"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	embed "github.com/D8-X/d8x-futures-go-sdk/config"
	"github.com/D8-X/d8x-futures-go-sdk/utils"

	"github.com/redis/rueidis"
)

// REDIS set name
const AVAIL_TICKER_SET string = "avail"
const TICKER_REQUEST = "request"
const PRICE_UPDATE_MSG = "px_update"

type DataPoint struct {
	Timestamp int64
	Value     float64
}

type RueidisClient struct {
	Client *rueidis.Client
	Ctx    context.Context
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

func (r *RueidisClient) Get(key string) (DataPoint, error) {
	vlast, err := (*r.Client).Do(r.Ctx, (*r.Client).B().TsGet().Key(key).Build()).ToArray()
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
		return []DataPoint{}, errors.New("invalid aggr type")
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

type SymbolManager struct {
	ConfigFile          ConfigFile
	PriceFeedIds        []utils.PriceFeedId
	PythIdToSym         map[string]string       //pyth id (0xabc..) to symbol (btc-usd)
	SymToPythOrigin     map[string]string       //symbol (btc-usd) to pyth origin ("Crypto.BTC/USD")
	CandlePeriodsMs     map[string]CandlePeriod //period 1m,5m,... to timeMs and displayRangeMs
	SymConstructionMutx *sync.Mutex             //mutex when data for a symbol is being constructed
}

type ConfigFile struct {
	PythAPIEndpoint       string   `json:"pythAPIEndpoint"`
	PythPriceEndpoints    []string `json:"priceServiceHTTPSEndpoints"`
	PredMktPriceEndpoints []string `json:"predMktPriceEndpoints"`
	ObsoleteWS            []string `json:"priceServiceWSEndpoints"`
}

// New initializes a new SymbolManager
func (sm *SymbolManager) New(fileName string) error {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &sm.ConfigFile)
	if err != nil {
		return err
	}
	if len(sm.ConfigFile.PythPriceEndpoints) == 0 {
		// legacy config
		sm.ConfigFile.PythPriceEndpoints = make([]string, len(sm.ConfigFile.ObsoleteWS))
		for k, ws := range sm.ConfigFile.ObsoleteWS {
			httpsAddr, _ := strings.CutPrefix(ws, "wss")
			httpsAddr, _ = strings.CutSuffix(httpsAddr, "/ws")
			httpsAddr = "https" + httpsAddr
			slog.Info(fmt.Sprintf("switching provided address %s to http-stream address %s", ws, httpsAddr))
			sm.ConfigFile.PythPriceEndpoints[k] = httpsAddr
		}
	}
	err = sm.extractPythIdToSymbolMap()
	if err != nil {
		return err
	}
	sm.SymConstructionMutx = &sync.Mutex{}
	err = sm.extractCandlePeriods()
	if err != nil {
		return err
	}
	return nil
}

// creates a map from ids "0x32121..." to symbols "xau-usd"
func (c *SymbolManager) extractPythIdToSymbolMap() error {
	slog.Info("Loading VAA ids for network PythEVMStable")
	config, err := embed.GetDefaultPriceConfigByName("PythEVMStable")
	if err != nil {
		return err
	}
	mIdToSym := make(map[string]string, len(config.PriceFeedIds))
	mSymToPythSym := make(map[string]string, len(config.PriceFeedIds))
	irrelevantSyms := make(map[string]struct{})
	for _, sym := range config.CandleIrrelevant {
		irrelevantSyms[strings.ToUpper(sym)] = struct{}{}
	}
	c.PriceFeedIds = make([]utils.PriceFeedId, 0, len(config.PriceFeedIds))
	for k, el := range config.PriceFeedIds {
		if el.Type != PYTH_TYPE {
			continue
		}
		s := strings.ToUpper(el.Symbol)
		if _, ok := irrelevantSyms[s]; ok {
			continue
		}
		idTrim, _ := strings.CutPrefix(el.Id, "0x")
		mIdToSym[idTrim] = s
		mSymToPythSym[s] = el.Origin
		c.PriceFeedIds = append(c.PriceFeedIds, config.PriceFeedIds[k])
	}
	c.PythIdToSym = mIdToSym
	c.SymToPythOrigin = mSymToPythSym
	return nil
}

func (c *SymbolManager) extractCandlePeriods() error {

	periods, err := config.GetCandlePeriodsConfig()
	if err != nil {
		return err
	}
	c.CandlePeriodsMs = make(map[string]CandlePeriod, len(periods))
	for _, el := range periods {
		p := strings.ToUpper(el.Period)
		c.CandlePeriodsMs[p] = CandlePeriod{Name: p, TimeMs: el.TimeMs, DisplayRangeMs: el.DisplayRangeMs}
	}
	return nil
}
