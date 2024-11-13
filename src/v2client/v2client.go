package v2client

import (
	"d8x-candles/src/globalrpc"
	"d8x-candles/src/uniutils"
	"d8x-candles/src/utils"
	"strings"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/rueidis"
)

type V2Client struct {
	Config            V2PoolConfig
	Ruedi             *rueidis.Client
	RpcHndl           *globalrpc.GlobalRpc
	RelevantPoolAddrs []common.Address
	Triangulations    map[string]d8x_futures.Triangulation //map index symbol to its triangulation
	PoolAddrToIndices map[string][]int                     // map pool address to price index location in Config.indices
	PoolAddrToSymbol  map[string]string                    //map pool address to its symbol
	SyncEventAbi      abi.ABI
}

func NewV2Client(configV2, configRpc, redisAddr, redisPw string) (*V2Client, error) {
	var v2 V2Client
	var err error
	v2.Config, err = loadV2PoolConfig(configV2)
	if err != nil {
		return nil, err
	}
	// ruedis client
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{redisAddr}, Password: redisPw})
	if err != nil {
		return nil, err
	}
	v2.Ruedi = &client
	v2.RpcHndl, err = globalrpc.NewGlobalRpc(configRpc, redisAddr, redisPw)
	if err != nil {
		return nil, err
	}
	v2.PoolAddrToIndices = make(map[string][]int)
	v2.PoolAddrToSymbol = make(map[string]string)
	for j, idx := range v2.Config.Indices {
		for k := 1; k < len(idx.Triang); k += 2 {
			sym := idx.Triang[k]
			for _, pool := range v2.Config.Pools {
				if pool.Symbol == sym {
					v2.PoolAddrToIndices[pool.PoolAddr.Hex()] = append(v2.PoolAddrToIndices[pool.PoolAddr.Hex()], j)
					v2.PoolAddrToSymbol[pool.PoolAddr.Hex()] = pool.Symbol
				}
			}
		}
	}
	v2.RelevantPoolAddrs = make([]common.Address, 0, len(v2.PoolAddrToIndices))
	for addr := range v2.PoolAddrToIndices {
		v2.RelevantPoolAddrs = append(v2.RelevantPoolAddrs, common.HexToAddress(addr))
	}
	// create relevant timeseries in Redis
	err = uniutils.InitRedisIndices(v2.Config.Indices, utils.TYPE_V2, &client)
	if err != nil {
		return nil, err
	}
	// abi
	v2.SyncEventAbi, err = abi.JSON(strings.NewReader(SYNC_EVENT_ABI))
	if err != nil {
		return nil, err
	}
	// triangulations
	v2.Triangulations = make(map[string]d8x_futures.Triangulation)
	for j := range v2.Config.Indices {
		v2.Triangulations[v2.Config.Indices[j].Symbol] =
			uniutils.TriangFromStringSlice(v2.Config.Indices[j].Triang)
	}

	return &v2, nil
}

func (v2 *V2Client) Run() error {
	return nil
}
