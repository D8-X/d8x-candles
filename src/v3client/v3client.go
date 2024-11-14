package v3client

import (
	"context"
	"d8x-candles/src/globalrpc"
	"d8x-candles/src/uniutils"
	"d8x-candles/src/utils"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/rueidis"
)

type V3Client struct {
	Config            *Config
	Ruedi             *rueidis.Client
	RpcHndl           *globalrpc.GlobalRpc
	RelevantPoolAddrs []common.Address                     // contains all pool addresses that are used for indices
	Triangulations    map[string]d8x_futures.Triangulation //map index symbol to its triangulation
	PoolAddrToIndices map[string][]int                     // map pool address to price index location in Config.indices
	PoolAddrToSymbol  map[string]string                    //map pool address to its symbol
	SwapEventAbi      abi.ABI
}

func NewV3Client(configV3, configRpc, redisAddr, redisPw string) (*V3Client, error) {
	var v3 V3Client
	var err error
	v3.Config, err = loadV2PoolConfig(configV3)
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
	v3.RpcHndl, err = globalrpc.NewGlobalRpc(configRpc, redisAddr, redisPw)
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
	for addr := range v3.PoolAddrToIndices {
		v3.RelevantPoolAddrs = append(v3.RelevantPoolAddrs, common.HexToAddress(addr))
	}
	// create relevant timeseries in Redis
	err = uniutils.InitRedisIndices(v3.Config.Indices, utils.TYPE_V3, &client)
	if err != nil {
		return nil, err
	}
	// abi
	v3.SwapEventAbi, err = abi.JSON(strings.NewReader(SWAP_EVENT_ABI))
	if err != nil {
		return nil, err
	}
	// triangulations
	v3.Triangulations = make(map[string]d8x_futures.Triangulation)
	for j := range v3.Config.Indices {
		v3.Triangulations[v3.Config.Indices[j].Symbol] =
			uniutils.TriangFromStringSlice(v3.Config.Indices[j].Triang)
	}
	return &v3, nil
}

func (v3 *V3Client) Run() error {
	v3.Filter()
	slog.Info("filtering historical v3 data complete")
	for j := range v3.Config.Indices {
		// set market hours for index symbol
		sym := v3.Config.Indices[j].Symbol
		utils.RedisSetMarketHours(v3.Ruedi, sym, utils.MarketHours{IsOpen: true, NextOpen: 0, NextClose: 0}, "crypto")
		// set index symbol as available in redis
		c := *v3.Ruedi
		c.Do(context.Background(), c.B().Sadd().Key(utils.AVAIL_TICKER_SET).Member(sym).Build())
	}

	for {

		rec, err := v3.RpcHndl.GetAndLockRpc(globalrpc.TypeWSS, 10)
		if err != nil {
			return err
		}
		client, err := ethclient.Dial(rec.Url)
		if err != nil {
			v3.RpcHndl.ReturnLock(rec)
			slog.Error("v3 failed to connect to the Ethereum client: " + err.Error())
			continue
		}
		err = v3.runWebsocket(client)
		if err != nil {
			slog.Error(err.Error())
		}
		v3.RpcHndl.ReturnLock(rec)
	}
}

func (v3 *V3Client) runWebsocket(client *ethclient.Client) error {
	swapSig := uniutils.GetEventSignatureHash(SWAP_EVENT_SIGNATURE)
	query := ethereum.FilterQuery{
		Addresses: v3.RelevantPoolAddrs,
		Topics:    [][]common.Hash{{swapSig}},
	}
	logs := make(chan types.Log)
	sub, err := client.SubscribeFilterLogs(context.Background(), query, logs)
	if err != nil {
		return fmt.Errorf("subscribing to event logs: %v", err)
	}
	fmt.Println("Listening for Uniswap V3 swap events...")
	for {
		select {
		case err := <-sub.Err():
			return fmt.Errorf("subscription: %v", err)
		case vLog := <-logs:
			// Check which event the log corresponds to
			addr := vLog.Address.Hex()
			switch vLog.Topics[0] {
			case swapSig:
				// Handle Swap event
				go v3.onSwap(addr, vLog)
			}
		}
	}
}

// onSwap handles a swap event in v3 client which leads
// to a price change
func (v3 *V3Client) onSwap(poolAddr string, log types.Log) {
	var event SwapEvent
	poolAddr = common.HexToAddress(poolAddr).Hex()
	sym, exists := v3.PoolAddrToSymbol[poolAddr]
	if !exists {
		slog.Error("pool addr not in universe", "addr", poolAddr)
		return
	}
	slog.Info("onSwap", "symbol", sym)
	err := v3.SwapEventAbi.UnpackIntoInterface(&event, "Swap", log.Data)
	if err != nil {
		slog.Error("failed to unpack Swap event", "error", err)
		return
	}
	price := SqrtPriceX96ToPrice(event.SqrtPriceX96)
	nowTs := time.Now().UnixMilli()
	err = utils.RedisAddPriceObs(v3.Ruedi, utils.TYPE_V3, sym, price, nowTs)
	if err != nil {
		slog.Error("onSwap: failed to insert new obs", "error", err)
		return
	}
	// triangulate
	idx := v3.PoolAddrToIndices[poolAddr]
	if len(idx) == 0 {
		fmt.Printf("pool %s no indices\n", poolAddr)
		return
	}
	symUpdated := ""
	for _, j := range idx {
		pxIdx := v3.Config.Indices[j]
		var px float64 = 1
		px, _, err := utils.RedisCalcTriangPrice(
			v3.Ruedi,
			utils.TYPE_V3,
			v3.Triangulations[pxIdx.Symbol],
		)
		if err != nil {
			fmt.Printf("onSwap: RedisCalcTriangPrice failed %v\n", err)
			return
		}
		// write the updated price
		err = utils.RedisAddPriceObs(v3.Ruedi, utils.TYPE_V3, pxIdx.Symbol, px, nowTs)
		if err != nil {
			slog.Error("onSwap: failed to RedisAddPriceObs", "error", err)
			return
		}
		symUpdated += utils.TYPE_V3.ToString() + ":" + pxIdx.Symbol + ";"
	}
	symUpdated = strings.TrimSuffix(symUpdated, ";")
	utils.RedisPublishIdxPriceChange(v3.Ruedi, symUpdated)
}
