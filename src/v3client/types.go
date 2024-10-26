package v3client

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"

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
	Indices []ConfigIndex `json:"indices"`
	Pools   []ConfigPool  `json:"pools"`
}

// Index represents each index in the "indices" array
type ConfigIndex struct {
	Symbol string   `json:"symbol"`
	Triang []string `json:"triang"`
}

// Pool represents each pool in the "pools" array
type ConfigPool struct {
	Symbol string `json:"symbol"`
	Addr   string `json:"addr"`
}

type RpcConfig struct {
	Wss   []string `json:"wss"`
	Https []string `json:"https"`
}

type BlockObs struct {
	blockNum int64
	ts       uint64 //timestamp
	symToPx  map[string]float64
}

func NewBlockObsArray(startBlock, endBlock int64) []BlockObs {
	obs := make([]BlockObs, endBlock-startBlock)
	for i := range obs {
		obs[i].blockNum = startBlock + int64(i)
		obs[i].symToPx = make(map[string]float64)
	}
	return obs
}

func loadConfig(filename string) (*Config, error) {
	// Read the file contents
	data, err := os.ReadFile(filename) // Use os.ReadFile in Go 1.16+
	if err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	// Unmarshal the JSON data into the Response struct
	var response Config
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}
	for j, pool := range response.Pools {
		// pool address -> pool data
		// avoid uppercase/lowercase issues by converting to
		// address and let the library choose uppercase/lowercase
		response.Pools[j].Addr = common.HexToAddress(pool.Addr).Hex()
	}

	return &response, nil
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
