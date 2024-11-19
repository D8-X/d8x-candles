package v2client

import (
	"d8x-candles/src/uniutils"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

const SYNC_EVENT_SIGNATURE = "Sync(uint112,uint112)"

type V2PoolConfig struct {
	ChainID    int                    `json:"chainId"`
	Factory    string                 `json:"factory"`
	Multicall  string                 `json:"multicall"`
	V2Router02 string                 `json:"v2Router02"`
	Indices    []uniutils.ConfigIndex `json:"indices"`
	Pools      []UniswapV2Pool        `json:"pools"`
}

type UniswapV2Pool struct {
	Symbol    string         `json:"symbol"`
	TokenAddr []string       `json:"tokenAddr"`
	PoolAddr  common.Address // calculated, not in config
	TokenDec  []uint8        `json:"tokenDec"` // ordered token decimals
}

// loadV2PoolConfig loads config/v2_pools.json.
func loadV2PoolConfig(filename string, chainId int) (*V2PoolConfig, error) {
	var allPools []V2PoolConfig
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()

	// Read the file's contents into a byte slice
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}
	// Unmarshal the JSON data
	err = json.Unmarshal(byteValue, &allPools)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
	}
	// find chain
	idx := -1
	for j, pools := range allPools {
		if pools.ChainID == chainId {
			idx = j
			break
		}
	}
	if idx == -1 {
		slog.Info("no V2 pool config defined for chain", "chain", chainId)
		return nil, nil
	}
	pools := allPools[idx]
	for j := range pools.Indices {
		pools.Indices[j].Symbol = strings.ToUpper(pools.Indices[j].Symbol)
		for k := 1; k < len(pools.Indices[j].Triang); k += 2 {
			pools.Indices[j].Triang[k] = strings.ToUpper(pools.Indices[j].Triang[k])
		}
	}
	for j := range pools.Pools {
		pools.Pools[j].Symbol = strings.ToUpper(pools.Pools[j].Symbol)
	}
	return &pools, nil
}
