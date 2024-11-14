package v2client

import (
	"context"
	"d8x-candles/src/globalrpc"
	"d8x-candles/src/uniutils"
	"d8x-candles/src/utils"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
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

// Run is the main entrance to v2client service
func (v2 *V2Client) Run() error {
	slog.Info("start filtering historical v2 events")
	v2.Filter()
	slog.Info("filtering historical v2 data complete")
	for j := range v2.Config.Indices {
		// set market hours for index symbol
		sym := v2.Config.Indices[j].Symbol
		utils.RedisSetMarketHours(v2.Ruedi, sym, utils.MarketHours{IsOpen: true, NextOpen: 0, NextClose: 0}, "crypto")
		// set index symbol as available in redis
		c := *v2.Ruedi
		c.Do(context.Background(), c.B().Sadd().Key(utils.AVAIL_TICKER_SET).Member(sym).Build())
	}

	for {

		rec, err := v2.RpcHndl.GetAndLockRpc(globalrpc.TypeWSS, 10)
		if err != nil {
			return err
		}
		client, err := ethclient.Dial(rec.Url)
		if err != nil {
			v2.RpcHndl.ReturnLock(rec)
			slog.Error("v2 failed to connect to the Ethereum client: " + err.Error())
			continue
		}
		err = v2.runWebsocket(client)
		if err != nil {
			slog.Error(err.Error())
		}
		v2.RpcHndl.ReturnLock(rec)
	}
}
func (v2 *V2Client) runWebsocket(client *ethclient.Client) error {
	//Emitted each time reserves are updated via mint, burn, swap, or sync.
	syncSig := uniutils.GetEventSignatureHash(SYNC_EVENT_SIGNATURE)

	query := ethereum.FilterQuery{
		Addresses: v2.RelevantPoolAddrs,
		Topics:    [][]common.Hash{{syncSig}},
	}
	// Subscribe to the events

	logs := make(chan types.Log)
	sub, err := client.SubscribeFilterLogs(context.Background(), query, logs)
	if err != nil {
		return fmt.Errorf("subscribing to event logs: %v", err)
	}

	fmt.Println("Listening for Uniswap V2 sync events...")

	for {
		select {
		case err := <-sub.Err():
			return fmt.Errorf("subscription: %v", err)
		case vLog := <-logs:
			// Check which event the log corresponds to
			switch vLog.Topics[0] {
			case syncSig:
				// Handle Swap event
				go v2.onSync(vLog)
			}
		}
	}
}

// onSync is called upon receipt of a Sync event (swap, add/remove liquidity)
func (v2 *V2Client) onSync(log types.Log) {
	addr := log.Address.Hex()
	var event SyncEvent
	err := v2.SyncEventAbi.UnpackIntoInterface(&event, "Sync", log.Data)
	if err != nil {
		slog.Error("v2 failed to unpack Swap event", "error", err)
		return
	}
	info := v2.PoolAddrToInfo[addr]
	r0 := uniutils.DecNToFloat(event.Reserve0, info.TokenDec[0])
	r1 := uniutils.DecNToFloat(event.Reserve1, info.TokenDec[1])
	px := r1 / r0
	slog.Info("onSync v2", "symbol", info.Symbol, "price", px)
	// store mid-price
	utils.RedisAddPriceObs(
		v2.Ruedi,
		utils.TYPE_V2,
		info.Symbol,
		px,
		time.Now().UnixMilli(),
	)
	// update related prices
	v2.idxPriceUpdate(addr)
}

// idxPriceUpdate updates all index prices that depend on the given
// pool
func (v2 *V2Client) idxPriceUpdate(poolAddr string) error {
	idx := v2.PoolAddrToIndices[poolAddr]
	if len(idx) == 0 {
		fmt.Printf("pool %s no indices\n", poolAddr)
		return nil
	}
	for j := range idx {
		pxIdx := v2.Config.Indices[j]
		var px float64 = 1
		px, oldestTs, err := utils.RedisCalcTriangPrice(
			v2.Ruedi,
			utils.TYPE_V2,
			v2.Triangulations[pxIdx.Symbol],
		)
		if err != nil {
			return err
		}
		symbol := pxIdx.Symbol
		err = utils.RedisAddPriceObs(
			v2.Ruedi,
			utils.TYPE_V2,
			symbol,
			px,
			oldestTs,
		)
		if err != nil {
			return err
		}
	}
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
