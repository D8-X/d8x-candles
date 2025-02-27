package uniutils

import (
	"d8x-candles/src/utils"
	"fmt"
	"log/slog"
	"strings"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/redis/rueidis"
)

// combineWithPyth combine uni prices with pyth, start at startMs
func (fltr *Filter) combineWithPyth(startMs int64, client *rueidis.Client) error {
	for _, idx := range fltr.Indices {
		idxSym := strings.Split(idx.Symbol, ".")[0]
		if idxSym == idx.FromPools {
			// index is obtained from pools entirely,
			// no multiplication with pyth price needed
			continue
		}
		if idx.FromPyth == "" {
			// configuration error
			return fmt.Errorf("config error: idx symbol not equal to fromPools but no fromPyth")
		}
		obs, err := utils.RedisGetRangeFrom(
			client,
			fltr.UniType,
			idx.FromPools,
			startMs,
		)
		if err != nil {
			return fmt.Errorf("unable to get range prices for %s: %v", idx.FromPools, err)
		}
		for _, o := range obs {
			ts, px, err := utils.RedisGetPriceObsBefore(
				client,
				d8xUtils.PXTYPE_PYTH,
				idx.FromPyth,
				o.TsMs,
			)
			if err != nil {
				slog.Error("no price found to create index", "symbol", idxSym, "ts", ts)
				continue
			}
			price := px * o.Px
			err = utils.RedisAddPriceObs(client, fltr.UniType, idxSym, price, o.TsMs)
			if err != nil {
				slog.Error("unable to add index price to redis", "symbol", idxSym, "error", err)
			}
		}
	}
	return nil
}
