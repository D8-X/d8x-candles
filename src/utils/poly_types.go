package utils

import (
	"encoding/json"
	"strconv"
)

type SymbolPoly struct {
	AssetType   string
	Symbol      string
	ConditionId string
	TokenId     string //CLOB token id
}

type PolyMarketInfo struct {
	Active          bool        `json:"active"`
	Closed          bool        `json:"closed"`
	AcceptingOrders bool        `json:"accepting_orders"`
	EndDateISOTs    int64       `json:"end_date_iso_ts"`
	Tokens          []PolyToken `json:"tokens"`
}

type PolyToken struct {
	TokenID string  `json:"token_id"`
	Outcome string  `json:"outcome"`
	Price   float64 `json:"price"`
	Winner  bool    `json:"winner"`
}

// Order represents an order with price and size.
type PolyOrder struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

// MarketResponse represents the JSON response structure.
type PolyMarketResponse struct {
	Asks      []PolyOrder `json:"asks"`
	AssetID   string      `json:"asset_id"`
	Bids      []PolyOrder `json:"bids"`
	EventType string      `json:"event_type"`
	Hash      string      `json:"hash"`
	Market    string      `json:"market"`
}

// CustomInt64 type for handling int64 unmarshalling from JSON string
type CustomInt64 int64
type CustomFloat64 float64

type PolyMidPrice struct {
	Px CustomFloat64 `json:"mid"`
}

type PolyHistoryResponse struct {
	History []PolyHistory `json:"history"`
}

type PolyHistory struct {
	TimestampSec int64   `json:"t"`
	Price        float64 `json:"p"`
}

// UnmarshalJSON implements the json.Unmarshaler interface for CustomInt64
func (ci *CustomInt64) UnmarshalJSON(data []byte) error {
	var timestampStr string
	if err := json.Unmarshal(data, &timestampStr); err != nil {
		return err
	}
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return err
	}
	*ci = CustomInt64(timestamp)
	return nil
}

// UnmarshalJSON implements the json.Unmarshaler interface for CustomInt64
func (ci *CustomFloat64) UnmarshalJSON(data []byte) error {
	var floatStr string
	if err := json.Unmarshal(data, &floatStr); err != nil {
		return err
	}
	num, err := strconv.ParseFloat(floatStr, 64)
	if err != nil {
		return err
	}
	*ci = CustomFloat64(num)
	return nil
}

// PolyPriceChange represents the JSON response structure for price change events.
type PolyPriceChange struct {
	AssetID     string       `json:"asset_id"`
	Changes     []PolyChange `json:"changes"`
	EventType   string       `json:"event_type"`
	Hash        string       `json:"hash"`
	Market      string       `json:"market"`
	TimestampMs CustomInt64  `json:"timestamp"`
}

type PolyChange struct {
	Price string `json:"price"`
	Side  string `json:"side"`
	Size  string `json:"size"`
}
