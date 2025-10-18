package sportsclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"d8x-candles/src/utils"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/gorilla/websocket"
)

type Envelope struct {
	Channel string    `json:"channel"`
	Data    GameEvent `json:"data"`
}

type GameEvent struct {
	Reftime    string `json:"reftime"` // ISO8601 string
	ContractID string `json:"contract_id"`
	// LeagueName   string   `json:"league_name"`
	// StartTime    string   `json:"start_time"` // ISO8601 string
	// AwayTeam     string   `json:"away_team"`
	// HomeTeam     string   `json:"home_team"`
	// Period1      string   `json:"period1"`
	// Period2      *string  `json:"period2"` // null -> nil
	// Period3      *string  `json:"period3"` // null -> nil
	// AwayScore    int      `json:"away_score"`
	// HomeScore    int      `json:"home_score"`
	AwayWinProb string `json:"away_win_prob"` // keep as string to avoid float issues
	HomeWinProb string `json:"home_win_prob"` // same
	// AwayTeamFull string   `json:"away_team_full"`
	// HomeTeamFull string   `json:"home_team_full"`
	EventStatus int    `json:"event_status"`
	IndexPrice  string `json:"indexprice"` // decimal as string
	// HomeWinLine  *float64 `json:"home_win_line"` // null -> nil
	// AwayWinLine  *float64 `json:"away_win_line"` // null -> nil
}

// listenWs connects and listens to the websocket, terminates on error
func (sp *SportsClient) listenWs(ctx context.Context) error {
	const WS_WAIT = 70 * time.Second
	c, _, err := websocket.DefaultDialer.Dial(sp.WsUrl, nil)
	if err != nil {
		return fmt.Errorf("dial: %s", err.Error())
	}
	defer c.Close()
	sp.Conn = c
	sp.Conn.SetReadDeadline(time.Now().Add(WS_WAIT))
	for {
		select {
		case <-ctx.Done():
			slog.Info("context cancel for ws-listener")
			return nil
		default:
			_, message, err := sp.Conn.ReadMessage()
			if err != nil {
				return fmt.Errorf("read err or connection closed: %w", err)
			}
			sp.Conn.SetReadDeadline(time.Now().Add(WS_WAIT))
			var v Envelope
			if err := json.Unmarshal(message, &v); err != nil {
				slog.Error("invalid message", "error", err, "message", message)
				continue
			}
			sp.handleMessage(&v)
		}
	}
}

func (sp *SportsClient) handleMessage(v *Envelope) {
	if v.Data.IndexPrice == "nil" {
		slog.Info("index price nil", "id", v.Data.ContractID)
	}
	px, err := strconv.ParseFloat(v.Data.IndexPrice, 64)
	if err != nil {
		slog.Error("invalid price", "price", v.Data.IndexPrice)
	}
	sym := v.Data.ContractID + "-USD"
	if !sp.KnownSymbols.Exists(sym) {
		const retentionMs = 86400000
		if err := utils.RedisCreateIfNotExistsTs(&sp.Ruedi, d8xUtils.PXTYPE_SPORT, sym, retentionMs); err != nil {
			slog.Error("unable to create redis ts", "error", err)
			return
		}
		sp.KnownSymbols.AddElement(sym)
	}
	slog.Info("price update", "contractId", v.Data.ContractID, "price", v.Data.IndexPrice, "event_status", v.Data.EventStatus)
	pxMark := px
	if v.Data.EventStatus == 1 {
		// contract live
		// we need ema
		prices, err := sp.SdkRO.FetchPricesForPerpetual(v.Data.ContractID, "")
		if err == nil {
			pxMark = prices.Ema - 1
		}
	}
	sp.OnNewPrice(sym, px, pxMark, time.Now().UnixMilli())
}

// OnNewPrice stores the new price in redis and informs subscribers
func (sp *SportsClient) OnNewPrice(sym string, px, pxMark float64, tsMs int64) {
	slog.Info("publishing new price", "sym", sym, "px", px)
	err := utils.RedisAddPriceObs(sp.Ruedi, d8xUtils.PXTYPE_SPORT, sym, px, tsMs)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to update price for %s in redis: %v", sym, err))
		return
	}
	err = utils.RedisAddPriceObs(sp.Ruedi, d8xUtils.PXTYPE_SPORT, sym+"|mark", pxMark, tsMs)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to update price for %s in redis: %v", sym, err))
		return
	}

	// publish update
	key := d8xUtils.PXTYPE_SPORT.String() + ":" + sym
	key = key + ";" + d8xUtils.PXTYPE_SPORT.String() + ":" + sym + "|mark"
	err = utils.RedisPublishIdxPriceChange(&sp.Ruedi, key)
	if err != nil {
		slog.Error("Redis Pub" + err.Error())
	}
}
