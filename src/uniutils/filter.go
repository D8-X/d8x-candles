package uniutils

import (
	"context"
	"d8x-candles/src/globalrpc"
	"d8x-candles/src/utils"
	"fmt"
	"log"
	"math/big"
	"slices"
	"strings"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/rueidis"
	"golang.org/x/exp/slog"
)

type BlockObs struct {
	Ts      uint64 //timestamp
	SymToPx map[string]float64
	Abi     abi.ABI //???

}

type Filter struct {
	Indices    []ConfigIndex
	UniType    utils.PriceType
	Prices     map[uint64]*BlockObs
	RpcHndl    *globalrpc.GlobalRpc
	Abi        abi.ABI
	EvtSigHash common.Hash
	handleFunc func(*Filter, types.Log)
}

func NewFilter(
	unitype utils.PriceType,
	indices []ConfigIndex,
	rpcHndl *globalrpc.GlobalRpc,
	eventAbi string,
	eventSigHash common.Hash,
	handleFunc func(*Filter, types.Log),
) (*Filter, error) {

	var f = Filter{
		Indices:    indices,
		UniType:    unitype,
		Prices:     make(map[uint64]*BlockObs),
		RpcHndl:    rpcHndl,
		EvtSigHash: eventSigHash,
		handleFunc: handleFunc,
	}
	var err error
	f.Abi, err = abi.JSON(strings.NewReader(eventAbi))
	if err != nil {
		return nil, err
	}

	return &f, nil
}

func (fltr *Filter) Run(client *rueidis.Client, relPoolAddr []common.Address) error {
	symToAdd := missingSymsInHist(fltr.Indices, fltr.UniType, client)
	if len(symToAdd) == 0 {
		slog.Info("no missing symbols in v3 history")
		return nil
	}
	nowTs := time.Now().Unix()
	start := nowTs - LOOKBACK_SEC
	blk, blkNow, err := fltr.findStartingBlock(uint64(start))
	if err != nil {
		return err
	}
	err = fltr.runFilterer(
		int64(blk),
		int64(blkNow),
		relPoolAddr,
	)
	if err != nil {
		return err
	}
	fltr.findBlockTs()
	// now triangulate available prices
	fltr.fillTriangulatedHistory()
	fltr.histPricesToRedis(symToAdd, client)

	return nil
}

// findStartingBlock finds the block with given timestamp. Returns the block found,
// the current block, and potentially an error
func (fltr *Filter) findStartingBlock(startTs uint64) (uint64, uint64, error) {
	var blk, blkNow, ts uint64
	var err error
	for trial := 0; trial < 3; trial++ {
		var rec globalrpc.Receipt
		rec, err = fltr.RpcHndl.GetAndLockRpc(globalrpc.TypeHTTPS, 15)
		if err != nil {
			fltr.RpcHndl.ReturnLock(rec)
			continue
		}
		client, err := ethclient.Dial(rec.Url)
		if err != nil {
			fltr.RpcHndl.ReturnLock(rec)
			slog.Info("error ethclient dial", "error", err, "url", rec.Url)
			continue
		}
		blk, ts, blkNow, err = FindBlockWithTs(client, uint64(startTs))
		if err != nil {
			fltr.RpcHndl.ReturnLock(rec)
			slog.Info("error finding block, retry", "error", err)
			continue
		}
		fltr.RpcHndl.ReturnLock(rec)
		break
	}
	if err != nil {
		return 0, 0, err
	}
	fmt.Printf("found block with ts %d (searched for ts %d) diff=%d\n", ts, startTs, max(startTs, ts)-min(startTs, ts))
	return blk, blkNow, nil
}

// RunFilterer collects swap events from startBlk to blockNow
// in BlockObs, without filling the block timestamp (requires separate rpc query)
func (fltr *Filter) runFilterer(
	startBlk int64,
	blockNow int64,
	poolAddr []common.Address,
) error {

	fromBlock := big.NewInt(startBlk)

	//toBlock := nil
	// Create filter query
	INC_BLOCK := int64(1000)
	toBlock := big.NewInt(fromBlock.Int64() + INC_BLOCK)
	for toBlock.Int64() != blockNow {
		toBlock = big.NewInt(fromBlock.Int64() + INC_BLOCK)
		if toBlock.Int64() > blockNow {
			toBlock = big.NewInt(blockNow)
		}
		query := ethereum.FilterQuery{
			FromBlock: fromBlock,
			ToBlock:   toBlock,
			Addresses: poolAddr,
			Topics:    [][]common.Hash{{fltr.EvtSigHash}},
		}
		var err error
		var logs []types.Log
		for trial := 0; trial < 3; trial++ {
			logs, err = getLogs(query, fltr.RpcHndl)
			if err != nil {
				fmt.Printf("\nerror %s, retry...\n", err.Error())
				time.Sleep(500 * time.Millisecond)
				continue
			}
			break
		}
		if err != nil {
			return err
		}
		// Process logs
		progress := 1 - float64(blockNow-fromBlock.Int64())/float64(blockNow-startBlk)
		fmt.Printf("\rprocessing %d swap events: %.2f", len(logs), progress)
		for _, vLog := range logs {
			fltr.handleFunc(fltr, vLog)
		}

		fromBlock = big.NewInt(toBlock.Int64() + 1)
	}
	log.Printf("\nfilterer processed.\n")
	return nil
}

// findBlockTs collects blocks where we have data and for
// which we need to figure out the timestamp
func (fltr *Filter) findBlockTs() {
	lastBlock := uint64(0)
	// we first loop through the map and get all blocks
	// into array blocks
	blocks := make([]uint64, len(fltr.Prices))
	j := 0
	for block := range fltr.Prices {
		blocks[j] = block
		j++
	}
	slices.Sort(blocks)
	// we fill some of the blocks with actual numbers

	for j, blockNum := range blocks {
		if blockNum-lastBlock > 1800 {
			ts, err := BlockTs(int64(blockNum), fltr.RpcHndl)
			if err != nil {
				//skip
				fmt.Println("error getting block timestamp, skipping")
				time.Sleep(500 * time.Millisecond)
				continue
			}
			lastBlock = blockNum
			fltr.Prices[blockNum].Ts = ts
			fmt.Printf("\rblock ts progress %.2f", float64(j)/float64(len(blocks)))
		}
	}
	// interpolate the rest
	interpolateTs(fltr.Prices)
}

// fillTriangulatedHistory amends the prices array by adding symbols
// and prices of triangulated symbols
func (fltr *Filter) fillTriangulatedHistory() {
	blocks := make([]uint64, len(fltr.Prices))
	j := 0
	for block := range fltr.Prices {
		blocks[j] = block
		j++
	}
	slices.Sort(blocks)

	for j := range fltr.Indices {
		triang := fltr.Indices[j].Triang
		sym2Triang := fltr.Indices[j].Symbol
		// store last price of underlying in map
		lastPx := make(map[string]float64)
		for k := 1; k < len(triang); k += 2 {
			lastPx[triang[k]] = float64(0)
		}
		for _, blockNum := range blocks {
			obs := fltr.Prices[blockNum]
			// see whether any of the underlying prices have
			// a change at this timestamp
			for sym := range lastPx {
				if v, exists := obs.SymToPx[sym]; exists {
					lastPx[sym] = v
				}
			}
			//triangulate
			px := float64(1)
			for k := 1; k < len(triang); k += 2 {
				p := lastPx[triang[k]]
				if p == 0 {
					// no price of underlying yet
					px = -1
					break
				}
				if triang[k-1] == "/" {
					px = px / p
				} else {
					px = px * p
				}
			}
			if px != -1 {
				fltr.Prices[blockNum].SymToPx[sym2Triang] = px
			}
		}
	}
}

// interpolateTs interpolates and extrapolates the timestamps
// given the filled data in BlockObs linearly
func interpolateTs(prices map[uint64]*BlockObs) {
	// we first loop through the map and get all blocks
	// into array blocks
	blocks := make([]uint64, len(prices))
	j := 0
	for block := range prices {
		blocks[j] = block
		j++
	}
	slices.Sort(blocks)
	var left int
	for j, currBlock := range blocks {
		if prices[currBlock].Ts != 0 {
			left = j
			break
		}
	}
	for j := left + 1; j < len(blocks); j++ {
		if prices[blocks[j]].Ts != 0 {
			if j-left == 1 {
				left = j
				continue
			}
			x1 := blocks[j]
			x0 := blocks[left]
			y1 := prices[x1].Ts
			y0 := prices[x0].Ts
			m := float64(y1-y0) / float64(x1-x0)
			for k := left; k < j; k++ {
				prices[blocks[k]].Ts = uint64(float64(y0) + m*(float64(blocks[k])-float64(x0)))
			}
			left = j
		}
	}
	// extrapolation
	// left end
	var pivot int
	for j := 0; j < len(blocks); j++ {
		if prices[blocks[j]].Ts != 0 {
			pivot = j
			break
		}
	}
	if pivot > 0 {
		// extrapolate left end
		x1 := blocks[pivot+1]
		x0 := blocks[pivot]
		y1 := float64(prices[x1].Ts)
		y0 := float64(prices[x0].Ts)
		m := float64(y1-y0) / float64(x1-x0)
		for k := 0; k < pivot; k++ {
			prices[blocks[k]].Ts = uint64(y0 + m*(float64(blocks[k])-float64(x0)))
		}
	}
	// right end
	for j := len(blocks) - 1; j > 0; j-- {
		if prices[blocks[j]].Ts != 0 {
			pivot = j
			break
		}
	}
	if pivot < len(blocks)-1 {
		// extrapolate right end
		x1 := float64(blocks[pivot])
		x0 := float64(blocks[pivot-1])
		y1 := float64(prices[blocks[pivot]].Ts)
		y0 := float64(prices[blocks[pivot-1]].Ts)
		m := float64(y1-y0) / float64(x1-x0)
		for k := pivot + 1; k < len(prices); k++ {
			prices[blocks[k]].Ts = uint64(y0 + m*(float64(blocks[k])-x0))
		}
	}
}

// histPricesToRedis adds prices (symbol, price per timestamp) available in `prices`
// if the symbol is to be addded
func (fltr *Filter) histPricesToRedis(symToAdd map[string]bool, client *rueidis.Client) error {
	for block, obs := range fltr.Prices {
		for sym, val := range obs.SymToPx {
			if _, exists := symToAdd[sym]; !exists {
				continue
			}
			err := utils.RedisAddPriceObs(client, fltr.UniType, sym, val, int64(obs.Ts*1000))
			if err != nil {
				return fmt.Errorf("insert triangulations to redis %s block=%d ts=%d: %v",
					sym, block, obs.Ts, err)
			}
		}
	}
	return nil
}

func getLogs(query ethereum.FilterQuery, rpcHndl *globalrpc.GlobalRpc) ([]types.Log, error) {
	rec, err := rpcHndl.GetAndLockRpc(globalrpc.TypeHTTPS, 15)
	defer rpcHndl.ReturnLock(rec)
	if err != nil {
		return nil, err
	}
	client, err := ethclient.Dial(rec.Url)
	if err != nil {
		rpcHndl.ReturnLock(rec)
		return nil, fmt.Errorf("error ethclient %s dial %v", rec.Url, err)
	}

	// Get logs
	logs, err := client.FilterLogs(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch logs with %s: %v", rec.Url, err)
	}
	return logs, nil
}
