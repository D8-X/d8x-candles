package uniutils

import (
	"d8x-candles/src/utils"
	"log/slog"
	"math/big"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/rueidis"
	"golang.org/x/crypto/sha3"
)

/*
	Functions shared between v2client and v3client

*/

// Index represents each index in the "indices" array
type ConfigIndex struct {
	Symbol       string   `json:"symbol"`
	Triang       []string `json:"triang"`
	FromPools    string   `json:"fromPools"` // we get the symbol 'fromPools' via triangulation
	FromPyth     string   `json:"fromPyth"`  // and multiply by a pyth price (e.g., HONEY-USD, USDC-USD)
	ContractSize float64  `json:"contractSize"`
}

const LOOKBACK_SEC = 86400 * 5 // now-LOOKBACK_SEC is when we start gathering history

func InitRedisIndices(indices []ConfigIndex, pxtype d8xUtils.PriceType, client *rueidis.Client) error {
	newSyms := make(map[string]bool)
	for j := range indices {
		// final price index symbol
		err := utils.RedisCreateIfNotExistsTs(client, pxtype, indices[j].Symbol)
		if err != nil {
			return err
		}
		newSyms[indices[j].Symbol] = true
		// triangulated index
		if indices[j].Symbol != indices[j].FromPools {
			err = utils.RedisCreateIfNotExistsTs(client, pxtype, indices[j].FromPools)
			if err != nil {
				return err
			}
			newSyms[indices[j].FromPools] = true
		}
		// triangulation components
		for k := 1; k < len(indices[j].Triang); k += 2 {
			err := utils.RedisCreateIfNotExistsTs(client, pxtype, indices[j].Triang[k])
			if err != nil {
				return err
			}
			newSyms[indices[j].Triang[k]] = true
		}
	}
	exSym := make([]string, len(newSyms))
	for sym := range newSyms {
		exSym = append(exSym, sym)
	}
	// we delete all other symbols as they are not in the current configuration
	return utils.RedisDelPrefix(client, pxtype.String(), exSym)
}

// TriangFromStringSlice creates a triangulation-type variable from
// a string-slice specifying a a triangulation
func TriangFromStringSlice(tr []string) d8x_futures.Triangulation {
	var triang d8x_futures.Triangulation
	triang.IsInverse = make([]bool, len(tr)/2)
	triang.Symbol = make([]string, len(tr)/2)
	for k := 1; k < len(tr); k += 2 {
		triang.IsInverse[k/2] = tr[k-1] == "/"
		triang.Symbol[k/2] = tr[k]
	}
	return triang
}

// cleanHist determines symbols for which historical data
// should be added to redis and cleans history. Symbols are indices
// and triangulation. 'FromPyth' is not part of theses symbols.
func cleanHist(
	indices []ConfigIndex,
	unitype d8xUtils.PriceType,
	maxAgeTsMs int64,
	client *rueidis.Client,
) map[string]bool {
	// identify all symbols
	symRequired := make(map[string]bool)
	for j := range indices {
		// FromPools contains the target symbol that
		// we get via triangulation
		symRequired[indices[j].FromPools] = true
		for k := 1; k < len(indices[j].Triang); k += 2 {
			symRequired[indices[j].Triang[k]] = true
		}
	}
	// find which symbols are missing in history
	// we clean historical data from the look-back moment
	for sym := range symRequired {
		err := utils.RedisCleanAfter(client, unitype, sym, maxAgeTsMs)
		if err != nil {
			slog.Error("failed to clean redis", "error", err)
		}
	}
	return symRequired
}

// GetEventSignatureHash calculates the event signature hash
func GetEventSignatureHash(eventSig string) common.Hash {
	hash := sha3.NewLegacyKeccak256()
	hash.Write([]byte(eventSig))
	return common.BytesToHash(hash.Sum(nil))
}

// DecNToFloat converts a decimal N number to
// the corresponding float number
func DecNToFloat(decN *big.Int, n uint8) float64 {
	y := new(big.Float).SetInt(decN)
	N := new(big.Int).SetUint64(uint64(n))
	pow := new(big.Int).Exp(big.NewInt(10), N, nil)
	q := new(big.Float).SetInt(pow)
	z := new(big.Float).Quo(y, q)
	f, _ := z.Float64()
	return f
}
