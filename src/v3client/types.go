package v3client

import (
	"d8x-candles/config"
	"d8x-candles/src/uniutils"
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

const SWAP_EVENT_SIGNATURE = "Swap(address,address,int256,int256,uint160,uint128,int24)"

type SwapEvent struct {
	// Sender       common.Address <-- indexed and not required
	// Recipient    common.Address <-- indexed and not required
	// example: https://bartio.beratrail.io/tx/0xc6e347c8011157288e90dbca344c7562cef06293f084d3f78f0b5dd3ee3e91ef/eventlog?chainid=80084
	Amount0      *big.Int
	Amount1      *big.Int
	SqrtPriceX96 *big.Int
	Liquidity    *big.Int
	Tick         *big.Int
}

type Config struct {
	ChainId     int                    `json:"chainId"`
	PoolChainId int                    `json:"poolChainId"`
	Indices     []uniutils.ConfigIndex `json:"indices"`
	Pools       []ConfigPool           `json:"pools"`
}

// Pool represents each pool in the "pools" array
type ConfigPool struct {
	Symbol   string  `json:"symbol"`
	Addr     string  `json:"addr"`
	TokenDec []uint8 `json:"tokenDec"`
}

type RpcConfig struct {
	Wss   []string `json:"wss"`
	Https []string `json:"https"`
}

// returns nil, nil if no config specified for given chain
func loadV3PoolConfig(chainId int, configFilePathOpt string) (*Config, error) {
	var byteValue []byte
	var err error
	// Read the file contents
	if configFilePathOpt != "" {
		byteValue, err = utils.ReadFile(configFilePathOpt)
	} else {
		byteValue, err = config.FetchConfigFromRepo("v3_idx_conf.json")
	}
	if err != nil {
		return nil, err
	}

	// Unmarshal the JSON data into the Response struct
	var configs []Config
	if err := json.Unmarshal(byteValue, &configs); err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}
	// find chain id
	idx := -1
	for j, config := range configs {
		if config.ChainId == chainId {
			idx = j
			break
		}
	}
	if idx == -1 {
		return nil, nil
	}
	config := configs[idx]
	// uppercase/lowercase
	for j, pool := range config.Pools {
		// pool address -> pool data
		// avoid uppercase/lowercase issues by converting to
		// address and let the library choose uppercase/lowercase
		config.Pools[j].Addr = common.HexToAddress(pool.Addr).Hex()
		// uppercase symbols
		config.Pools[j].Symbol = strings.ToUpper(config.Pools[j].Symbol)
	}
	// uppercase symbols
	for j := range config.Indices {
		config.Indices[j].Symbol = strings.ToUpper(config.Indices[j].Symbol)
		for k := 1; k < len(config.Indices[j].Triang); k += 2 {
			config.Indices[j].Triang[k] = strings.ToUpper(config.Indices[j].Triang[k])
		}
	}
	return &config, nil
}

func LoadRPCConfig(filename string) (RpcConfig, error) {
	var rpc RpcConfig
	jsonFile, err := os.Open(filename)
	if err != nil {
		return RpcConfig{}, err
	}
	defer jsonFile.Close()

	// Read the file's contents into a byte slice
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}
	// Unmarshal the JSON data
	err = json.Unmarshal(byteValue, &rpc)
	if err != nil {
		return RpcConfig{}, fmt.Errorf("error unmarshalling JSON: %v", err)
	}
	return rpc, nil
}
