package v2client

import (
	"d8x-candles/src/globalrpc"
	"d8x-candles/src/uniutils"
	"d8x-candles/src/utils"
	"encoding/hex"
	"math/big"
	"strings"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/rueidis"
	"golang.org/x/crypto/sha3"
)

type SyncEvent struct {
	//event Sync(uint112 reserve0, uint112 reserve1);
	Reserve0 *big.Int
	Reserve1 *big.Int
}

type V2Client struct {
	Config            V2PoolConfig
	Ruedi             *rueidis.Client
	RpcHndl           *globalrpc.GlobalRpc
	RelevantPoolAddrs []common.Address
	Triangulations    map[string]d8x_futures.Triangulation //map index symbol to its triangulation
	PoolAddrToIndices map[string][]int                     // map pool address to price index location in Config.indices
	PoolAddrToInfo    map[string]UniswapV2Pool
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
	// calculate v2 pool address
	fct := common.HexToAddress(v2.Config.Factory)
	for j, pool := range v2.Config.Pools {
		t0 := common.HexToAddress(pool.TokenAddr[0])
		t1 := common.HexToAddress(pool.TokenAddr[1])
		v2.Config.Pools[j].PoolAddr = calcV2PoolAddr(t0, t1, fct)
	}

	v2.PoolAddrToIndices = make(map[string][]int)
	v2.PoolAddrToInfo = make(map[string]UniswapV2Pool)
	for j, idx := range v2.Config.Indices {
		for k := 1; k < len(idx.Triang); k += 2 {
			sym := idx.Triang[k]
			for _, pool := range v2.Config.Pools {
				if pool.Symbol == sym {
					v2.PoolAddrToIndices[pool.PoolAddr.Hex()] = append(v2.PoolAddrToIndices[pool.PoolAddr.Hex()], j)
					v2.PoolAddrToInfo[pool.PoolAddr.Hex()] = pool
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
	v2.Filter()
	return nil
}

// calcV2PoolAddr calculates the pool address (called 'Pair' in uniswap docs), from
// the two tokens and the factory. No rpc call.
func calcV2PoolAddr(token0, token1, factory common.Address) common.Address {
	t0 := token0.Hex()
	t1 := token1.Hex()
	fct := factory.Hex()
	initCodeHash := "96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f"
	// Decode factory, token0, token1, and initCodeHash to bytes
	factoryBytes, _ := hex.DecodeString(fct[2:])
	token0Bytes, _ := hex.DecodeString(t0[2:])
	token1Bytes, _ := hex.DecodeString(t1[2:])
	initCodeHashBytes, _ := hex.DecodeString(initCodeHash)

	// Compute keccak256(abi.encodePacked(token0, token1))
	tokenHash := sha3.NewLegacyKeccak256()
	tokenHash.Write(append(token0Bytes, token1Bytes...))
	tokenHashBytes := tokenHash.Sum(nil)

	// Compute keccak256(abi.encodePacked(hex'ff', factory, tokenHash, initCodeHash))
	data := append([]byte{0xff}, append(factoryBytes, append(tokenHashBytes, initCodeHashBytes...)...)...)
	finalHash := sha3.NewLegacyKeccak256()
	finalHash.Write(data)
	finalHashBytes := finalHash.Sum(nil)

	// Convert to an Ethereum address (last 20 bytes of the hash)
	pairAddress := finalHashBytes[12:]
	return common.BytesToAddress(pairAddress)
}
