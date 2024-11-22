package uniutils

import (
	"d8x-candles/src/utils"
	"math/big"
	"time"

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
	ContractSize float64  `json:"contractSize"`
}

const LOOKBACK_SEC = 86400 * 5 // now-LOOKBACK_SEC is when we start gathering history

func InitRedisIndices(indices []ConfigIndex, pxtype d8xUtils.PriceType, client *rueidis.Client) error {
	for j := range indices {
		err := utils.RedisCreateIfNotExistsTs(client, pxtype, indices[j].Symbol)
		if err != nil {
			return err
		}
		for k := 1; k < len(indices[j].Triang); k += 2 {
			err := utils.RedisCreateIfNotExistsTs(client, pxtype, indices[j].Triang[k])
			if err != nil {
				return err
			}
		}

	}
	return nil
}

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

// missingSymsInHist determines symbols for which historical data
// should be added to redis
func missingSymsInHist(indices []ConfigIndex, unitype d8xUtils.PriceType, client *rueidis.Client) map[string]bool {
	// identify all symbols
	symRequired := make(map[string]bool)
	for j := range indices {
		symRequired[indices[j].Symbol] = true
		for k := 1; k < len(indices[j].Triang); k += 2 {
			symRequired[indices[j].Triang[k]] = true
		}
	}
	// find which symbols are missing in history
	maxAgeTs := time.Now().UnixMilli() - LOOKBACK_SEC
	symToAdd := make(map[string]bool)
	for sym := range symRequired {
		ts := utils.RedisGetFirstTimestamp(client, unitype, sym)
		if ts == 0 || ts > maxAgeTs {
			symToAdd[sym] = true
		}
	}
	return symToAdd
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
