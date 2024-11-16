package pythclient

import (
	"context"
	"d8x-candles/src/utils"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
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

type PythClientApp struct {
	BaseUrl     string
	RedisClient *utils.RueidisClient
	TokenBucket *utils.TokenBucket
	SymbolMngr  *utils.SymbolManager
	MsgCount    map[string]int
	StreamMngr  StreamManager
}

func NewPythClientApp(symMngr *utils.SymbolManager, REDIS_ADDR string, REDIS_PW string) (*PythClientApp, error) {

	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{REDIS_ADDR}, Password: REDIS_PW})
	if err != nil {
		return nil, fmt.Errorf("redis connection %s", err.Error())
	}
	redisTSClient := &utils.RueidisClient{
		Client: &client,
		Ctx:    context.Background(),
	}
	//https://docs.pyth.network/benchmarks/rate-limits
	capacity := 30
	refillRate := 9.0
	tb := utils.NewTokenBucket(capacity, refillRate)
	pca := PythClientApp{
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
	err = setCCYAvailable(symMngr, &client)
	if err != nil {
		return nil, err
	}
	pca.clearPythTickerAvailability()
	return &pca, nil
}

func setCCYAvailable(symMngr *utils.SymbolManager, ruedi *rueidis.Client) error {
	syms, err := symMngr.ExtractCCY(d8xUtils.PXTYPE_PYTH)
	if err != nil {
		return err
	}
	return utils.RedisSetCcyAvailable(ruedi, d8xUtils.PXTYPE_PYTH, syms)
}

// symMap maps pyth ids to internal symbol (btc-usd)
func Run(symMngr *utils.SymbolManager, REDIS_ADDR string, REDIS_PW string) error {
	fmt.Print("REDIS ADDR = ", REDIS_ADDR)
	fmt.Print("REDIS_PW=", REDIS_PW)
	ph, err := NewPythClientApp(symMngr, REDIS_ADDR, REDIS_PW)
	if err != nil {
		return err
	}
	go ph.ScheduleMktInfoUpdate(15 * time.Minute)
	go ph.ScheduleCompaction(20 * time.Minute)

	errChan := make(chan error)
	go ph.SubscribeTickerRequest(errChan)
	err = <-errChan
	return err
}

// Schedule regular calls of compaction (e.g. every hour)
func (p *PythClientApp) ScheduleCompaction(waitTime time.Duration) {
	tickerUpdate := time.NewTicker(waitTime)
	for {
		<-tickerUpdate.C
		utils.CompactAllPriceObs(p.RedisClient.Client, d8xUtils.PXTYPE_PYTH)
		slog.Info("Compaction completed.")
	}
}

// clearPythTickerAvailability removes all pyth tickers
// from available tickers in REDIS
func (p *PythClientApp) clearPythTickerAvailability() {
	p.SymbolMngr.SymConstructionMutx.Lock()
	defer p.SymbolMngr.SymConstructionMutx.Unlock()
	// clean ticker availability
	cl := *p.RedisClient.Client
	config := p.SymbolMngr.PriceFeedIds
	key := utils.RDS_AVAIL_TICKER_SET + ":" + d8xUtils.PXTYPE_PYTH.String()
	for _, ids := range config {
		if ids.Type != d8xUtils.PXTYPE_PYTH {
			continue
		}
		fmt.Printf("deleting availability for %s\n", ids.Symbol)
		cl.Do(context.Background(), cl.B().Srem().Key(key).Member(ids.Symbol).Build())
	}
}
