package polyclient

import (
	"context"
	"d8x-candles/src/utils"
	"fmt"
	"log/slog"
	"sync"

	"github.com/redis/rueidis"
)

// SubscribeTickerRequest redis pub/sub utils.TICKER_REQUEST
// makes ticker available if possible
func (p *PolyClient) SubscribeTickerRequest(errChan chan error) {
	client := *p.RedisClient.Client
	err := client.Receive(context.Background(), client.B().Subscribe().Channel(utils.TICKER_REQUEST).Build(),
		func(msg rueidis.PubSubMessage) {
			p.enableTicker(msg.Message)
		})
	if err != nil {
		errChan <- err
	}
}

// HistoryToRedis adds the historical data in obs for the given
// symbol sym to redis and sets the ticker as available
func (p *PolyClient) HistoryToRedis(sym string, obs []utils.PolyHistory) {
	var wg sync.WaitGroup
	for k := 0; k < len(obs); k++ {
		// store prices in ms
		val := obs[k].Price
		t := int64(obs[k].TimestampSec) * 1000
		wg.Add(1)
		go func(sym string, t int64, val float64) {
			defer wg.Done()
			utils.RedisAddPriceObs(p.RedisClient.Client, sym, val, t)
		}(sym, t, val)
	}
	// set the symbol as available
	c := *p.RedisClient.Client
	fmt.Printf("make %s available in REDIS\n", sym)
	c.Do(context.Background(), c.B().Sadd().Key(utils.AVAIL_TICKER_SET).Member(sym).Build())
	wg.Wait()
}

// OnNewPrice stores the new price in redis and informs subscribers
func (p *PolyClient) OnNewPrice(sym string, px float64, tsMs int64) {
	err := utils.RedisAddPriceObs(p.RedisClient.Client, sym, px, tsMs)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to update price for %s in redis: %v", sym, err))
		return
	}

	// publish updates to listeners
	err = utils.RedisPublishPriceChange(p.RedisClient.Client, sym)
	if err != nil {
		slog.Error("Redis Pub" + err.Error())
	}
}
