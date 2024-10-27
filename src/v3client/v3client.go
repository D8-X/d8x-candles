package v3client

import (
	"d8x-candles/src/utils"

	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/rueidis"
)

type V3Client struct {
	Config            *Config
	Ruedi             *rueidis.Client
	RpcHndl           *RpcHandler
	RelevantPoolAddrs []common.Address  // contains all pool addresses that are used for indices
	PoolAddrToIndices map[string][]int  // map pool address to price index location in Config.indices
	PoolAddrToSymbol  map[string]string //map pool address to its symbol
}

func NewV3Client(configV3, configRpc, redisAddr, redisPw string) (*V3Client, error) {
	var v3 V3Client
	var err error
	v3.Config, err = loadConfig(configV3)
	if err != nil {
		return nil, err
	}
	// ruedis client
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{redisAddr}, Password: redisPw})
	if err != nil {
		return nil, err
	}
	v3.Ruedi = &client
	v3.RpcHndl, err = NewRpcHandler(configRpc)
	if err != nil {
		return nil, err
	}
	v3.PoolAddrToIndices = make(map[string][]int)
	v3.PoolAddrToSymbol = make(map[string]string)
	for j, idx := range v3.Config.Indices {
		for k := 1; k < len(idx.Triang); k += 2 {
			sym := idx.Triang[k]
			for _, pool := range v3.Config.Pools {
				if pool.Symbol == sym {
					v3.PoolAddrToIndices[pool.Addr] = append(v3.PoolAddrToIndices[pool.Addr], j)
					v3.PoolAddrToSymbol[pool.Addr] = pool.Symbol
				}
			}
		}
	}
	v3.RelevantPoolAddrs = make([]common.Address, 0, len(v3.PoolAddrToIndices))
	for addr, _ := range v3.PoolAddrToIndices {
		v3.RelevantPoolAddrs = append(v3.RelevantPoolAddrs, common.HexToAddress(addr))
	}
	// create relevant timeseries in Redis
	for j := range v3.Config.Indices {
		err := utils.RedisCreateIfNotExistsTs(&client, v3.Config.Indices[j].Symbol)
		if err != nil {
			return nil, err
		}
		for k := 1; k < len(v3.Config.Indices[j].Triang); k += 2 {
			err := utils.RedisCreateIfNotExistsTs(&client, v3.Config.Indices[j].Triang[k])
			if err != nil {
				return nil, err
			}
		}

	}
	return &v3, nil
}
