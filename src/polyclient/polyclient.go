package polyclient

import (
	"context"
	"d8x-candles/src/utils"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	stork "github.com/D8-X/d8x-futures-go-sdk/pkg/stork"
	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
)

type PolyClient struct {
	RedisClient       *utils.RueidisClient
	api               *PolyApi
	priceFeedUniverse map[string]d8xUtils.PriceFeedId
	activeSyms        map[string]ActiveState
	muSyms            *sync.RWMutex
	stork             *stork.Stork
}

type ActiveState uint8

const (
	MARKET_CLOSED ActiveState = iota
	MARKET_ACTIVE
)

func (pc *PolyClient) Run() error {
	if len(pc.priceFeedUniverse) == 0 {
		slog.Info("no polymarket tickers in universe")
		return nil
	}

	// open websocket connection
	stopCh := make(chan struct{})
	go pc.api.RunWs(stopCh, pc)

	// schedule market info updates
	go pc.ScheduleMktInfoUpdate(5 * time.Minute)

	errChan := make(chan error)
	go pc.SubscribeTickerRequest(errChan)
	err := <-errChan
	return err
}

// cleanPythTickerAvailability removes all pyth tickers
// from the set of available tickers
func (p *PolyClient) cleanPolyTickerAvailability() {

	// clean ticker availability
	cl := *p.RedisClient.Client
	for _, ids := range p.priceFeedUniverse {
		if ids.Type != utils.POLYMARKET_TYPE {
			continue
		}
		fmt.Printf("deleting availability in REDIS for %s\n", ids.Symbol)
		cl.Do(context.Background(), cl.B().Srem().Key(utils.AVAIL_TICKER_SET).Member(ids.Symbol).Build())
	}
}

func NewPolyClient(oracleEndpt, REDIS_ADDR, REDIS_PW, storkEndpoint, storkCredentials string, config []d8xUtils.PriceFeedId) (*PolyClient, error) {
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{REDIS_ADDR}, Password: REDIS_PW})
	if err != nil {
		return nil, fmt.Errorf("redis connection %s", err.Error())
	}
	var pc PolyClient
	pc.RedisClient = &utils.RueidisClient{
		Client: &client,
		Ctx:    context.Background(),
	}
	if storkEndpoint != "" && storkCredentials != "" {
		pc.stork = stork.NewStork(storkEndpoint, storkCredentials)
	}
	// data for polymarket api
	pc.api = NewPolyApi(oracleEndpt)
	// available ticker universe

	// now add
	pc.priceFeedUniverse = make(map[string]d8xUtils.PriceFeedId, 0)
	for _, el := range config {
		if el.Type != utils.POLYMARKET_TYPE {
			continue
		}
		sym := strings.ToUpper(el.Symbol)
		fmt.Printf("adding ticker %s to universe\n", sym)
		pc.priceFeedUniverse[sym] = el
	}
	// clean availability
	pc.cleanPolyTickerAvailability()

	pc.activeSyms = make(map[string]ActiveState)
	pc.muSyms = &sync.RWMutex{}
	return &pc, nil
}

// enableTicker makes the given ticker available:
// gathers historical data, sets the ticker available in REDIS,
// subscribes to the websocket for this ticker
func (p *PolyClient) enableTicker(sym string) {
	el, exists := p.priceFeedUniverse[sym]
	if !exists {
		slog.Info(fmt.Sprintf("ticker request for symbol %s -- not in polymarket universe", sym))
		return
	}
	p.muSyms.RLock()
	if _, exists = p.activeSyms[sym]; exists {
		p.muSyms.RUnlock()
		slog.Info(fmt.Sprintf("ticker %s requested already exists", sym))
		return
	}
	p.muSyms.RUnlock()

	fmt.Printf("enable ticker request %s\n", sym)
	decId, err := utils.Hex2Dec(el.Id)
	if err != nil {
		slog.Error(fmt.Sprintf("could not convert hex-id to dec for %s: %v", sym, err))
		return
	}
	// query market
	m, err := GetMarketInfo(p.api.apiBucket, el.Origin)
	if err != nil {
		slog.Error(fmt.Sprintf("could not get market info %s: %v", sym, err))
		return
	}
	p.muSyms.Lock()
	if m.Closed {
		p.activeSyms[sym] = MARKET_CLOSED
	} else {
		p.activeSyms[sym] = MARKET_ACTIVE
	}
	p.muSyms.Unlock()

	utils.RedisReCreateTimeSeries(p.RedisClient.Client, sym)

	// set symbol available
	c := *p.RedisClient.Client
	c.Do(context.Background(), c.B().Sadd().Key(utils.AVAIL_TICKER_SET).Member(sym).Build())
	p.FetchMktInfo([]string{sym})
	if m.Closed {
		// market is closed
		p.setMarketClosed(sym, decId, m)
		return
	}
	// market open
	h, err := RestQueryHistory(p.api.apiBucket, decId)
	if err != nil {
		slog.Error(fmt.Sprintf("could not construct history for %s: %v", sym, err))
		return
	}
	p.HistoryToRedis(sym, h)
	p.api.SubscribeAssetIds([]string{decId}, []string{sym})

}

func (p *PolyClient) setMarketClosed(sym string, decId string, m *utils.PolyMarketInfo) {
	p.muSyms.Lock()
	if p.activeSyms[sym] == MARKET_CLOSED {
		p.muSyms.Unlock()
		return
	}
	p.activeSyms[sym] = MARKET_CLOSED
	p.muSyms.Unlock()
	var price float64
	if m.Tokens[0].TokenID == decId {
		price = m.Tokens[0].Price
	} else if m.Tokens[1].TokenID == decId {
		price = m.Tokens[1].Price
	} else {
		slog.Error(fmt.Sprintf("price info not found in closed market info %s", sym))
		return
	}
	p.OnNewPrice(sym, price, m.EndDateISOTs)
}

// Runs FetchMktHours and schedules next runs
func (p *PolyClient) ScheduleMktInfoUpdate(updtInterval time.Duration) {
	tickerUpdate := time.NewTicker(updtInterval)
	for {
		slog.Info("Updating polymarket info...")
		// get entire universe
		syms := make([]string, 0, len(p.priceFeedUniverse))
		for _, p := range p.priceFeedUniverse {
			syms = append(syms, p.Symbol)
		}
		p.FetchMktInfo(syms)
		fmt.Printf("Polymarket info updated for %d symbols.\n", len(syms))
		// wait for next time "tick"
		<-tickerUpdate.C
	}
}

// FetchMktInfo gets the market hours from the polymarket-api
// and stores the data to Redis
func (p *PolyClient) FetchMktInfo(syms []string) {
	// gather market ids for given symbols
	for _, sym := range syms {
		el, exists := p.priceFeedUniverse[sym]
		if !exists {
			slog.Error(fmt.Sprintf("FetchMktInfo: %s not in price feed universe", sym))
			continue
		}
		// condition id is in 'origin'
		m, err := GetMarketInfo(p.api.apiBucket, el.Origin)
		if err != nil {
			slog.Info(fmt.Sprintf("FetchMktInfo: id %s: %v", el.Origin, err))
			continue
		}
		if m.Closed {
			// market is closed, ensure we set the price accordingly
			decId, err := utils.Hex2Dec(el.Id)
			if err != nil {
				slog.Error(fmt.Sprintf("could not convert hex-id to dec for %s: %v", sym, err))
				continue
			}
			p.setMarketClosed(sym, decId, m)
		}
		isOpen := m.Active && !m.Closed && m.AcceptingOrders
		hrs := utils.MarketHours{
			IsOpen:    isOpen,
			NextOpen:  0,
			NextClose: m.EndDateISOTs,
		}
		utils.SetMarketHours(p.RedisClient, sym, hrs, utils.POLYMARKET_TYPE)
	}
}
