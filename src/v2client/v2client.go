package v2client

import (
	"context"
	"d8x-candles/src/globalrpc"
	"d8x-candles/src/uniutils"
	"d8x-candles/src/utils"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/rueidis"
)

type SyncEvent struct {
	//event Sync(uint112 reserve0, uint112 reserve1);
	Reserve0 *big.Int
	Reserve1 *big.Int
}

type V2Client struct {
	Config            *V2PoolConfig
	ConfigPyth        utils.UniPythConfig
	Ruedi             *rueidis.Client
	RpcHndl           *globalrpc.GlobalRpc
	RelevantPoolAddrs []common.Address
	Triangulations    map[string]d8x_futures.Triangulation //map index symbol to its triangulation
	PoolAddrToIndices map[string][]int                     // map pool address to price index location in Config.indices
	PoolAddrToInfo    map[string]UniswapV2Pool
	SyncEventAbi      abi.ABI
	LastUpdateTs      int64 //timestamp seconds
	MuLastUpdate      sync.RWMutex
}

func (v2 *V2Client) SetLastUpdateTs(tsSec int64) {
	v2.MuLastUpdate.Lock()
	defer v2.MuLastUpdate.Unlock()
	v2.LastUpdateTs = tsSec
}

func (v2 *V2Client) GetLastUpdateTs() int64 {
	v2.MuLastUpdate.RLock()
	defer v2.MuLastUpdate.RUnlock()
	return v2.LastUpdateTs
}

func NewV2Client(
	configRpc string,
	redisAddr string,
	redisPw string,
	chainId int,
	optV2Config string,
) (*V2Client, error) {

	var v2 V2Client
	var err error
	v2.Config, err = loadV2PoolConfig(chainId, optV2Config)
	if err != nil {
		return nil, err
	}

	// ruedis client
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{redisAddr}, Password: redisPw})
	if err != nil {
		return nil, fmt.Errorf("unable to start redis client %v", err)
	}
	v2.Ruedi = &client
	v2.RpcHndl, err = globalrpc.NewGlobalRpc(configRpc, v2.Config.PoolChainId, redisAddr, redisPw)
	if err != nil {
		return nil, err
	}
	// calculate v2 pool address
	fct := common.HexToAddress(v2.Config.Factory)
	for j, pool := range v2.Config.Pools {
		t0 := common.HexToAddress(pool.TokenAddr[0])
		t1 := common.HexToAddress(pool.TokenAddr[1])
		v2.Config.Pools[j].PoolAddr, err = getV2PairAddress(v2.RpcHndl, fct, t0, t1)
		if err != nil {
			return nil, fmt.Errorf("unable to getV2PairAddress: %v", err)
		}
		fmt.Printf("pool %s addr=%s\n", v2.Config.Pools[j].Symbol, v2.Config.Pools[j].PoolAddr.Hex())
	}

	v2.PoolAddrToIndices = make(map[string][]int)
	v2.PoolAddrToInfo = make(map[string]UniswapV2Pool)
	v2.ConfigPyth, err = utils.LoadUniPythConfig("") //load from remote
	if err != nil {
		return nil, err
	}
	for j, idx := range v2.Config.Indices {
		symP := "Crypto." + strings.Replace(idx.FromPyth, "-", "/", 1)
		if idx.FromPyth != "" && !slices.Contains(v2.ConfigPyth.Indices, symP) {
			return nil, fmt.Errorf("index %s not defined in uni_pyth config", idx.FromPyth)
		}
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
	err = uniutils.InitRedisIndices(v2.Config.Indices, d8xUtils.PXTYPE_V2, &client)
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
	slog.Info("start filtering events")
	err := v2.Filter()
	if err != nil {
		return fmt.Errorf("unable to run filterer: %v", err)
	}
	slog.Info("filtering complete")
	key := utils.RDS_AVAIL_TICKER_SET + ":" + d8xUtils.PXTYPE_V2.String()
	// clear available tickers
	c := *v2.Ruedi
	c.Do(context.Background(), c.B().Del().Key(key).Build())
	for j := range v2.Config.Indices {
		// set market hours for index symbol
		sym := v2.Config.Indices[j].Symbol
		utils.RedisSetMarketHours(v2.Ruedi, sym, utils.MarketHours{IsOpen: true, NextOpen: 0, NextClose: 0}, d8xUtils.ACLASS_CRYPTO)
		// set index symbol as available in redis
		fmt.Printf("setting %s available\n", sym)
		c.Do(context.Background(), c.B().Sadd().Key(key).Member(sym).Build())
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
			time.Sleep(2 * time.Second)
		}
		v2.RpcHndl.ReturnLock(rec)
		time.Sleep(2 * time.Second)
	}
}

func (v2 *V2Client) runWebsocket(client *ethclient.Client) error {
	slog.Info("starting new websocket connection")
	//Emitted each time reserves are updated via mint, burn, swap, or sync.
	syncSig := uniutils.GetEventSignatureHash(SYNC_EVENT_SIGNATURE)

	query := ethereum.FilterQuery{
		Addresses: v2.RelevantPoolAddrs,
		Topics:    [][]common.Hash{{syncSig}},
	}
	// Subscribe to the events

	logs := make(chan types.Log)
	ctx, cancel := context.WithCancel(context.Background())

	sub, err := client.SubscribeFilterLogs(ctx, query, logs)
	if err != nil {
		cancel()
		close(logs)
		return fmt.Errorf("subscribing to event logs: %v", err)
	}

	slog.Info("Listening for Uniswap V2 sync events...")
	//closing connection
	defer func() {
		slog.Info("Closing websocket subscription")
		sub.Unsubscribe()
		cancel()
		close(logs)
		client.Close()
	}()

	// timer to refresh pyth triangulation x pool prices
	refreshTick := time.NewTimer(time.Second * 10)
	defer refreshTick.Stop()
	// timer for inactivity of pool
	inactvTick := time.NewTicker(time.Minute * 1)
	defer inactvTick.Stop()
	for {
		select {
		case err := <-sub.Err():
			return fmt.Errorf("subscription: %v", err)
		case <-refreshTick.C:
			slog.Info("refreshing triangulations with Pyth prices")
			v2.refreshIdxWithPyth()
		case <-inactvTick.C:
			dT := time.Now().Unix() - v2.GetLastUpdateTs()
			if dT > 2*60 {
				// restart
				slog.Info("no updates received triggering websocket restart")
				return nil
			}
		case vLog := <-logs:
			v2.SetLastUpdateTs(time.Now().Unix())
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
		d8xUtils.PXTYPE_V2,
		info.Symbol,
		px,
		time.Now().UnixMilli(),
	)
	// update related prices
	v2.idxPriceUpdate(addr)
}

// refreshIdxWithPyth updates the triangulations that involve pyth prices
func (v2 *V2Client) refreshIdxWithPyth() {
	symUpdated := ""
	for _, index := range v2.Config.Indices {
		if index.FromPyth == "" {
			continue
		}
		px, ts, err := v2.triangulateIndex(index)
		if err != nil {
			slog.Info("re-triangulation failed for symbol", "symbol", index.Symbol, "error", err)
			continue
		}
		err = utils.RedisAddPriceObs(
			v2.Ruedi,
			d8xUtils.PXTYPE_V2,
			index.Symbol,
			px,
			ts,
		)
		if err != nil {
			slog.Info("re-triangulation failed for symbol", "symbol", index.Symbol, "error", err)
			continue
		}
		symUpdated += d8xUtils.PXTYPE_V2.String() + ":" + index.Symbol + ";"
	}
	symUpdated = strings.TrimSuffix(symUpdated, ";")
	utils.RedisPublishIdxPriceChange(v2.Ruedi, symUpdated)
}

// idxPriceUpdate updates all index prices that depend on the given
// pool
func (v2 *V2Client) idxPriceUpdate(poolAddr string) error {
	idx := v2.PoolAddrToIndices[poolAddr]
	if len(idx) == 0 {
		fmt.Printf("pool %s no indices\n", poolAddr)
		return nil
	}
	symUpdated := ""
	for j := range idx {
		// scale price
		px, oldestTs, err := v2.triangulateIndex(v2.Config.Indices[j])
		if err != nil {
			slog.Error("triangulateIndex", "error", err)
			continue
		}
		sym := v2.Config.Indices[j].Symbol
		err = utils.RedisAddPriceObs(
			v2.Ruedi,
			d8xUtils.PXTYPE_V2,
			sym,
			px,
			oldestTs,
		)
		if err != nil {
			return err
		}
		symUpdated += d8xUtils.PXTYPE_V2.String() + ":" + sym + ";"
	}
	symUpdated = strings.TrimSuffix(symUpdated, ";")
	utils.RedisPublishIdxPriceChange(v2.Ruedi, symUpdated)
	return nil
}

func (v2 *V2Client) triangulateIndex(index uniutils.ConfigIndex) (float64, int64, error) {
	var px float64 = 1
	px, oldestTs, err := utils.RedisCalcTriangPrice(
		v2.Ruedi,
		d8xUtils.PXTYPE_V2,
		v2.Triangulations[index.Symbol],
	)
	if err != nil {
		return 0, 0, err
	}
	// if fromPyth is defined we multiply the triangulation by
	// that symbol price
	pxPth := float64(1)
	fromPyth := index.FromPyth
	if fromPyth != "" {
		p, err := utils.RedisTsGet(v2.Ruedi, fromPyth, d8xUtils.PXTYPE_PYTH)
		if err != nil {
			return 0, 0, fmt.Errorf("unable to get price fromPyth=%s %v", fromPyth, err)
		}
		pxPth = p.Value
		oldestTs = min(oldestTs, p.Timestamp)
	}
	// scale price
	px = px * pxPth * index.ContractSize
	return px, oldestTs, nil
}

// getV2PairAddress queries the Uniswap V2 factory for the pair address
// This could be computed too, however Bera mainnet seems to function differently
func getV2PairAddress(rpcH *globalrpc.GlobalRpc, factoryAddress common.Address, tokenA common.Address, tokenB common.Address) (common.Address, error) {
	// Uniswap V2 Factory ABI (only getPair function)
	const factoryABI = `[{"constant":true,"inputs":[{"name":"tokenA","type":"address"},{"name":"tokenB","type":"address"}],"name":"getPair","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}]`
	r, err := rpcH.GetAndLockRpc(globalrpc.TypeHTTPS, 5)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed get rpc handler: %w", err)
	}
	defer rpcH.ReturnLock(r)
	client, err := ethclient.Dial(r.Url)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to connect to Ethereum client: %w", err)
	}

	// Ensure tokenA < tokenB for correct ordering
	if tokenA.Big().Cmp(tokenB.Big()) > 0 {
		tokenA, tokenB = tokenB, tokenA
	}

	// Parse ABI
	parsedABI, err := abi.JSON(strings.NewReader(factoryABI))
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to parse ABI: %w", err)
	}

	// Pack function call data
	data, err := parsedABI.Pack("getPair", tokenA, tokenB)
	if err != nil {
		return common.Address{}, fmt.Errorf("failed to encode getPair call: %w", err)
	}

	// Create call message
	msg := ethereum.CallMsg{
		To:   &factoryAddress,
		Data: data,
	}

	// Perform eth_call
	result, err := client.CallContract(context.Background(), msg, nil)
	if err != nil {
		return common.Address{}, fmt.Errorf("eth_call failed: %w", err)
	}

	// Decode returned address
	pairAddress := common.BytesToAddress(result)
	if pairAddress == (common.Address{}) {
		return common.Address{}, fmt.Errorf("no pair found for tokens %s and %s", tokenA.Hex(), tokenB.Hex())
	}

	return pairAddress, nil
}
