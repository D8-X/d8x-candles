package polyclient

import (
	"context"
	"d8x-candles/src/utils"
	"fmt"
	"log/slog"
	"sync"
	"time"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
)

type PolyClient struct {
	RedisClient       *utils.RueidisClient
	api               *PolyApi
	priceFeedUniverse map[string]d8xUtils.PriceFeedId
	activeSyms        map[string]bool
	muSyms            *sync.RWMutex
}

func (pc *PolyClient) Run() error {
	if len(pc.priceFeedUniverse) == 0 {
		slog.Info("no polymarket tickers in universe")
		return nil
	}
	// open websocket connection
	stopCh := make(chan struct{})
	go pc.api.RunWs(stopCh, pc)

	// schedule market info updates
	go pc.ScheduleMktInfoUpdate(15 * time.Minute)

	errChan := make(chan error)
	go pc.SubscribeTickerRequest(errChan)
	err := <-errChan
	return err
}

func NewPolyClient(REDIS_ADDR, REDIS_PW string, config []d8xUtils.PriceFeedId) (*PolyClient, error) {
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
	// data for polymarket api
	pc.api = NewPolyApi()
	// available ticker universe
	pc.priceFeedUniverse = make(map[string]d8xUtils.PriceFeedId, 0)
	for _, el := range config {
		if el.Type != utils.POLYMARKET_TYPE {
			continue
		}
		fmt.Printf("adding ticker %s to universe", el.Symbol)
		pc.priceFeedUniverse[el.Symbol] = el
	}
	pc.activeSyms = make(map[string]bool)
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

	fmt.Printf("enable ticker request %s", sym)
	decId, err := utils.Hex2Dec(el.Id)
	if err != nil {
		slog.Error(fmt.Sprintf("could not convert hex-id to dec for %s: %v", sym, err))
		return
	}
	h, err := RestQueryHistory(p.api.apiBucket, decId)
	if err != nil {
		slog.Error(fmt.Sprintf("could not construct history for %s: %v", sym, err))
		return
	}
	utils.CreateRedisTimeSeries(p.RedisClient, sym)
	p.HistoryToRedis(sym, h)
	p.api.SubscribeAssetIds([]string{decId}, []string{sym})
}

// Runs FetchMktHours and schedules next runs
func (p *PolyClient) ScheduleMktInfoUpdate(updtInterval time.Duration) {
	tickerUpdate := time.NewTicker(updtInterval)
	for {
		<-tickerUpdate.C

		slog.Info("Updating polymarket info...")
		p.muSyms.RLock()
		syms := make([]string, 0, len(p.activeSyms))
		for s := range p.activeSyms {
			syms = append(syms, s)
		}
		p.muSyms.RUnlock()
		p.FetchMktInfo(syms)
		fmt.Println("Polymarket info updated.")
	}
}

// FetchMktInfo gets the market hours from the polymarket-api
// and stores the data to Redis
func (p *PolyClient) FetchMktInfo(syms []string) {
	// gather market ids for given symbols
	mktIds := make([]string, 0, len(syms))
	usedSyms := make([]string, 0, len(syms))
	for _, sym := range syms {
		el, exists := p.priceFeedUniverse[sym]
		if !exists {
			slog.Error(fmt.Sprintf("FetchMktInfo: %s not in price feed universe", sym))
			continue
		}
		// condition id is in 'origin'
		mktIds = append(mktIds, el.Origin)
		usedSyms = append(usedSyms, sym)
	}
	hours := p.api.FetchMktHours(mktIds)
	for k, mh := range hours {
		utils.SetMarketHours(p.RedisClient, usedSyms[k], mh, utils.POLYMARKET_TYPE)
	}
}