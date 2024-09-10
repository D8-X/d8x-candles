package pythclient

import (
	"context"
	"d8x-candles/src/utils"
	"fmt"
	"sync"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	"github.com/redis/rueidis"
)

type PythStream struct {
	Id string  `json:"id"`
	P  float64 `json:"p"`
	T  uint32  `json:"t"`
	F  string  `json:"f"`
	S  int8    `json:"s"`
}

type SubscribeRequest struct {
	Type string   `json:"type"`
	IDs  []string `json:"ids"`
}

// symMap maps pyth ids to internal symbol (btc-usd)
func Run(symMngr *utils.SymbolManager, REDIS_ADDR string, REDIS_PW string) error {

	fmt.Print("REDIS ADDR = ", REDIS_ADDR)
	fmt.Print("REDIS_PW=", REDIS_PW)

	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{REDIS_ADDR}, Password: REDIS_PW})
	if err != nil {
		return fmt.Errorf("redis connection %s", err.Error())
	}
	redisTSClient := &utils.RueidisClient{
		Client: &client,
		Ctx:    context.Background(),
	}
	//https://docs.pyth.network/benchmarks/rate-limits
	capacity := 30
	refillRate := 9.0
	tb := utils.NewTokenBucket(capacity, refillRate)
	ph := PythClientApp{
		BaseUrl:     symMngr.ConfigFile.PythAPIEndpoint,
		RedisClient: redisTSClient,
		TokenBucket: tb,
		SymbolMngr:  symMngr,
		MsgCount:    make(map[string]int),
		StreamMngr: StreamManager{
			mu:                   &sync.Mutex{},
			asymRWMu:             &sync.RWMutex{},
			lastPxRWMu:           &sync.RWMutex{},
			s2DTRWMu:             &sync.RWMutex{},
			symToTriangPathRWMu:  &sync.RWMutex{},
			activeSyms:           make(map[string]int64),
			lastPx:               make(map[string]float64),
			symToDependentTriang: make(map[string][]string),
			SymToTriangPath:      make(map[string]d8x_futures.Triangulation),
		},
	}
	ph.cleanPythTickerAvailability()
	go ph.ScheduleMktInfoUpdate(15 * time.Minute)
	go ph.ScheduleCompaction(20 * time.Minute)

	errChan := make(chan error)
	go ph.SubscribeTickerRequest(errChan)
	err = <-errChan
	return err
	/*
		slog.Info("Building price history...")
		ph.BuildHistory()
		go ph.ScheduleMktInfoUpdate(15 * time.Minute)
		go ph.ScheduleCompaction(15 * time.Minute)

		err = ph.streamHttp(symMngr.ConfigFile.PythPriceEndpoints, symMap)
		if err != nil {
			return err
		}
		return nil
	*/
}

// cleanPythTickerAvailability removes all pyth tickers
// from the set of available tickers
func (p *PythClientApp) cleanPythTickerAvailability() {
	p.SymbolMngr.SymConstructionMutx.Lock()
	defer p.SymbolMngr.SymConstructionMutx.Unlock()
	// clean ticker availability
	cl := *p.RedisClient.Client
	config := p.SymbolMngr.PriceFeedIds
	for _, ids := range config {
		if ids.Type != utils.PYTH_TYPE {
			continue
		}
		fmt.Printf("deleting availability for %s\n", ids.Symbol)
		cl.Do(context.Background(), cl.B().Srem().Key(utils.AVAIL_TICKER_SET).Member(ids.Symbol).Build())
	}
}
