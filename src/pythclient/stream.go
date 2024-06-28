package pythclient

import (
	"bufio"
	"context"
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	"github.com/redis/rueidis"
)

// StreamManager manages the HTTP stream
type StreamManager struct {
	cancelFunc           context.CancelFunc
	mu                   *sync.Mutex
	activeSyms           map[string]int64 //symbol->timestamp
	asymRWMu             *sync.RWMutex
	lastPxRWMu           *sync.RWMutex
	lastPx               map[string]float64
	symToDependentTriang map[string][]string
	s2DTRWMu             *sync.RWMutex
	SymToTriangPath      map[string]d8x_futures.Triangulation //sym to triangulation path
	symToTriangPathRWMu  *sync.RWMutex
}

func (s *StreamManager) AddSymbolToTriangTarget(symT string, path *d8x_futures.Triangulation) {
	s.s2DTRWMu.Lock()
	for j := 0; j < len(path.Symbol); j++ {
		s.symToDependentTriang[path.Symbol[j]] = append(s.symToDependentTriang[path.Symbol[j]], symT)
	}
	s.s2DTRWMu.Unlock()
}

// SubscribeTickerRequest the to redis pub/sub utils.TICKER_REQUEST
// makes triangulation or direct ticker available if possible
func (p *PythClientApp) SubscribeTickerRequest(errChan chan error) {
	client := *p.RedisClient.Client
	err := client.Receive(context.Background(), client.B().Subscribe().Channel(utils.TICKER_REQUEST).Build(),
		func(msg rueidis.PubSubMessage) {
			p.enableTicker(msg.Message, errChan)
		})
	if err != nil {
		errChan <- err
	}
}

// enableTicker makes triangulation and/or base ticker available
func (ph *PythClientApp) enableTicker(sym string, errChan chan error) {
	ph.StreamMngr.asymRWMu.RLock()
	_, exists := ph.StreamMngr.activeSyms[sym]
	ph.StreamMngr.asymRWMu.RUnlock()
	if exists {
		slog.Info(fmt.Sprintf("ticker %s requested already exists", sym))
		return
	}
	slog.Info("Enabling ticker: " + sym)
	ph.EnableTriangulation(sym)
}

// EnableTriangulation constructs candles from existing candles
// that correspond to a price source (e.g. BTC-USD but not BTC-USDC)
// and enables that the candles will be maintained from this point on
func (p *PythClientApp) EnableTriangulation(symT string) bool {
	p.SymbolMngr.SymConstructionMutx.Lock()
	defer p.SymbolMngr.SymConstructionMutx.Unlock()
	config := p.SymbolMngr

	p.StreamMngr.symToTriangPathRWMu.RLock()
	_, exists := p.StreamMngr.SymToTriangPath[symT]
	p.StreamMngr.symToTriangPathRWMu.RUnlock()
	if exists {
		// already exists
		return true
	}
	path := d8x_futures.Triangulate(symT, config.PriceFeedIds)
	if len(path.Symbol) == 0 {
		slog.Info("Could not triangulate " + symT)
		return false
	}
	// which new symbols do we need to add to the universe?
	newSyms := make([]string, 0)
	p.StreamMngr.asymRWMu.RLock()
	for _, sym := range path.Symbol {
		_, exists := p.StreamMngr.activeSyms[sym]
		if !exists {
			newSyms = append(newSyms, sym)
		}
	}
	p.StreamMngr.asymRWMu.RUnlock()
	err := p.enableBaseTickers(newSyms)
	if err != nil {
		slog.Error("error enableBaseTickers " + symT + ":" + err.Error())
		return false
	}
	isBaseTicker := (len(path.Symbol) == 1) && !path.IsInverse[0]
	if isBaseTicker {
		return true
	}
	p.StreamMngr.AddSymbolToTriangTarget(symT, &path)
	p.StreamMngr.symToTriangPathRWMu.Lock()
	p.StreamMngr.SymToTriangPath[symT] = path
	p.StreamMngr.symToTriangPathRWMu.Unlock()

	p.fetchTriangulatedMktInfo()
	o, err := p.ConstructPriceObsForTriang(p.RedisClient, symT, path)
	if err != nil {
		slog.Error("error for triangulation " + symT + ":" + err.Error())
		return false
	}
	p.PricesToRedis(symT, o)
	return true
}

// enableBaseTickers adds new tickers to the universe of
// streamed tickers, gathers market info, restarts streaming
func (p *PythClientApp) enableBaseTickers(symbols []string) error {
	p.FetchMktInfo(symbols)
	err := p.BuildHistory(symbols)
	if err != nil {
		return err
	}
	// add symbols to active tickers
	now := time.Now().Unix()
	p.StreamMngr.asymRWMu.Lock()
	for _, sym := range symbols {
		p.StreamMngr.activeSyms[sym] = now
	}
	p.StreamMngr.asymRWMu.Unlock()
	// start streaming
	go p.StartStream()
	return nil
}

// StartStream starts a new HTTP stream in a goroutine
func (p *PythClientApp) StartStream() {
	p.StreamMngr.mu.Lock()
	if p.StreamMngr.cancelFunc != nil {
		p.StreamMngr.cancelFunc() // Cancel the previous stream if it exists
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.StreamMngr.cancelFunc = cancel
	p.StreamMngr.mu.Unlock()

	go func() {
		if err := p.streamHttpWithRetry(ctx); err != nil {
			slog.Info("Stream: " + err.Error())
		}
	}()
}

// streamHttpWithRetry handles the HTTP stream with retry logic
func (p *PythClientApp) streamHttpWithRetry(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			slog.Info("Stream canceled")
			return ctx.Err()
		default:
			endpoints := p.SymbolMngr.ConfigFile.PythPriceEndpoints
			ep := endpoints[rand.Intn(len(endpoints))]
			err := p.streamHttp(ctx, ep)
			if err != nil {
				slog.Info(fmt.Sprintf("Stream cancelled (%s): %s", ep, err.Error()))
				time.Sleep(1 * time.Second) // Wait for a short period before retrying
				continue
			}
			return nil
		}
	}
}

// streamHttp streams HTTP data and processes it
func (ph *PythClientApp) streamHttp(ctx context.Context, endpoint string) error {

	//https://hermes.pyth.network/docs/#/
	postfix := "/v2/updates/price/stream?parsed=true&allow_unordered=true&benchmarks_only=true&encoding=base64"
	ph.StreamMngr.asymRWMu.RLock()
	for sym := range ph.StreamMngr.activeSyms {
		id := ""
		for id0, sym0 := range ph.SymbolMngr.PythIdToSym {
			if sym0 == sym {
				id = id0
				break
			}
		}
		if id == "" {
			return fmt.Errorf("streamHttp: no id for symbol %s", sym)
		}
		postfix += "&ids[]=" + "0x" + id
	}
	ph.StreamMngr.asymRWMu.RUnlock()

	url, _ := strings.CutSuffix(endpoint, "/")
	url += postfix
	slog.Info("Connecting to " + url)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("streaming not successful: %s", err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-200 status code %d", resp.StatusCode)
	}

	reader := bufio.NewReader(resp.Body)
	for {
		select {
		case <-ctx.Done():
			slog.Info("Stream canceled")
			return ctx.Err()
		default:
			line, err := reader.ReadBytes('\n')
			if err != nil {
				if err.Error() == "EOF" {
					// End of stream
					return nil
				}
				return fmt.Errorf("error reading stream: %v", err)
			}
			if len(line) > 3 && string(line[0:3]) == ":No" || len(line) < 3 {
				continue
			}
			var response utils.PythStreamData
			jsonData := string(line[5:])
			err = json.Unmarshal([]byte(jsonData), &response)
			if err != nil {
				fmt.Println("Error unmarshalling JSON:", err)
				continue
			}
			// Process the data
			for _, parsed := range response.Parsed {
				ph.OnPriceUpdate(parsed.Price, parsed.Id)
			}
		}
	}
}
