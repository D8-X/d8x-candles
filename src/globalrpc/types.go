package globalrpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

type RpcConfig struct {
	ChainId int      `json:"chainId"`
	Wss     []string `json:"wss"`
	Https   []string `json:"https"`
}

type RPCType int

const (
	TypeHTTPS RPCType = iota // Starts at 0
	TypeWSS                  // 1
)

func (t RPCType) String() string {
	switch t {
	case TypeHTTPS:
		return "HTTPS"
	case TypeWSS:
		return "WSS"
	default:
		return "Unknown"
	}
}

func loadRPCConfig(filename string) (RpcConfig, error) {
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
