package v3client

import (
	"context"
	"d8x-candles/src/utils"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/slog"
)

const LOOKBACK_SEC = 86400 * 5 // now-LOOKBACK_SEC is when we start gathering history

func (v3 *V3Client) Filter() error {

	symToAdd := v3.missingSymsInHist()
	if len(symToAdd) == 0 {
		slog.Info("no missing symbols in v3 history")
		return nil
	}
	nowTs := time.Now().Unix()
	start := nowTs - LOOKBACK_SEC
	blk, blkNow, err := v3.findStartingBlock(uint64(start))
	if err != nil {
		return err
	}
	prices, err := v3.runFilterer(int64(blk), int64(blkNow))
	if err != nil {
		return err
	}
	v3.findBlockTs(prices)
	// now triangulate available prices
	v3.fillTriangulatedHistory(prices)
	// finally insert to redis
	v3.histPricesToRedis(prices, symToAdd)

	return nil
}

// histPricesToRedis adds prices (symbol, price per timestamp) available in `prices`
// if the symbol is to be addded
func (v3 *V3Client) histPricesToRedis(prices []BlockObs, symToAdd map[string]bool) error {
	for t := range prices {
		for sym, val := range prices[t].symToPx {
			if _, exists := symToAdd[sym]; !exists {
				continue
			}
			err := utils.RedisAddPriceObs(v3.Ruedi, sym, val, int64(prices[t].ts*1000))
			if err != nil {
				return fmt.Errorf("insert triangulations to redis %s t=%d ts=%d: %v",
					sym, t, prices[t].ts, err)
			}
		}
	}
	return nil
}

// missingSymsInHist determines symbols for which historical data
// should be added to redis
func (v3 *V3Client) missingSymsInHist() map[string]bool {
	// identify all symbols
	symRequired := make(map[string]bool)
	for j := range v3.Config.Indices {
		symRequired[v3.Config.Indices[j].Symbol] = true
		for k := 1; k < len(v3.Config.Indices[j].Triang); k += 2 {
			symRequired[v3.Config.Indices[j].Triang[k]] = true
		}
	}
	// find which symbols are missing in history
	maxAgeTs := time.Now().UnixMilli() - LOOKBACK_SEC
	symToAdd := make(map[string]bool)
	for sym := range symRequired {
		ts := utils.RedisGetFirstTimestamp(v3.Ruedi, sym)
		if ts == 0 || ts > maxAgeTs {
			symToAdd[sym] = true
		}
	}
	return symToAdd
}

// fillTriangulatedHistory amends the prices array by adding symbols
// and prices of triangulated symbols
func (v3 *V3Client) fillTriangulatedHistory(prices []BlockObs) {
	for j := range v3.Config.Indices {
		triang := v3.Config.Indices[j].Triang
		sym2Triang := v3.Config.Indices[j].Symbol
		// store last price of underlying in map
		lastPx := make(map[string]float64)
		for k := 1; k < len(triang); k += 2 {
			lastPx[triang[k]] = float64(0)
		}
		for t := range prices {
			hasObs := false
			// see whether any of the underlying prices have
			// a change at this timestamp
			for sym := range lastPx {
				if v, exists := prices[t].symToPx[sym]; exists {
					lastPx[sym] = v
					hasObs = true
				}
			}
			if hasObs {
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
					prices[t].symToPx[sym2Triang] = px
				}
			}
		}
	}
}

// findBlockTs collects blocks where we have data and for
// which we need to figure out the timestamp
func (v3 *V3Client) findBlockTs(prices []BlockObs) {
	lastBlock := int64(0)
	// we fill some of the blocks with actual numbers
	for j, p := range prices {
		if len(p.symToPx) > 0 && p.blockNum-lastBlock > 1800 {
			ts, err := v3.blockTs(p.blockNum)
			if err != nil {
				//skip
				fmt.Println("error getting block timestamp, skipping")
				time.Sleep(500 * time.Millisecond)
				continue
			}
			lastBlock = p.blockNum
			prices[j].ts = ts
			fmt.Printf("\rblock ts progress %.2f", float64(j)/float64(len(prices)))
		}
	}
	// interpolate the rest
	interpolateTs(prices)
}

// interpolateTs interpolates and extrapolates the timestamps
// given the filled data in BlockObs linearly
func interpolateTs(prices []BlockObs) {
	var left int
	for j := 0; j < len(prices); j++ {
		if prices[j].ts != 0 {
			left = j
			break
		}
	}
	for j := left + 1; j < len(prices); j++ {
		if prices[j].ts != 0 {
			if j-left == 1 {
				left = j
				continue
			}
			x1 := prices[j].blockNum
			x0 := prices[left].blockNum
			y1 := prices[j].ts
			y0 := prices[left].ts
			m := float64(y1-y0) / float64(x1-x0)
			for k := left; k < j; k++ {
				prices[k].ts = y0 + uint64(m*float64(prices[k].blockNum-x0))
			}
			left = j
		}
	}
	// extrapolation
	// left end
	var pivot int
	for j := 0; j < len(prices); j++ {
		if prices[j].ts != 0 {
			pivot = j
			break
		}
	}
	if pivot > 0 {
		// extrapolate left end
		x1 := prices[pivot+1].blockNum
		x0 := prices[pivot].blockNum
		y1 := prices[pivot+1].ts
		y0 := prices[pivot].ts
		m := float64(y1-y0) / float64(x1-x0)
		for k := 0; k < pivot; k++ {
			prices[k].ts = y0 + uint64(m*float64(prices[k].blockNum-x0))
		}
	}
	// right end
	for j := len(prices) - 1; j > 0; j-- {
		if prices[j].ts != 0 {
			pivot = j
			break
		}
	}
	if pivot < len(prices)-1 {
		// extrapolate right end
		x1 := prices[pivot].blockNum
		x0 := prices[pivot-1].blockNum
		y1 := prices[pivot].ts
		y0 := prices[pivot-1].ts
		m := float64(y1-y0) / float64(x1-x0)
		for k := pivot + 1; k < len(prices); k++ {
			prices[k].ts = y0 + uint64(m*float64(prices[k].blockNum-x0))
		}
	}
}

// blockTs gets the block timestamp of the given block number using
// several trials
func (v3 *V3Client) blockTs(blockNum int64) (uint64, error) {
	var rec Receipt
	var err error
	var client *ethclient.Client
	ctx := context.Background()
	for trial := 0; trial < 3; trial++ {
		rec, err = v3.RpcHndl.WaitForRpc(TYPE_HTTPS, 15)
		defer v3.RpcHndl.ReturnLock(rec)
		if err != nil {
			continue
		}
		client, err = ethclient.Dial(rec.Url)
		if err != nil {
			slog.Info("error ethclient dial", "error", err, "url", rec.Url)
			continue
		}
		blk, err := BlockByNumberL2Compat(client, ctx, big.NewInt(blockNum))
		if err != nil {
			slog.Info("error BlockByNumberL2Compat", "error", err, "url", rec.Url)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return blk.Time(), nil
	}
	return 0, nil
}

// runFilterer collects swap events from startBlk to blockNow
// in BlockObs, without filling the block timestamp (requires separate rpc query)
func (v3 *V3Client) runFilterer(startBlk, blockNow int64) ([]BlockObs, error) {

	swapSig := getEventSignatureHash(SWAP_EVENT_SIGNATURE)

	// Load the Swap event ABI
	swapABI, _ := abi.JSON(strings.NewReader(SWAP_EVENT_ABI))
	fromBlock := big.NewInt(startBlk)

	prices := NewBlockObsArray(startBlk, blockNow)
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
			Addresses: v3.RelevantPoolAddrs,
			Topics:    [][]common.Hash{{swapSig}},
		}
		var err error
		var logs []types.Log
		for trial := 0; trial < 3; trial++ {
			logs, err = v3.getLogs(query)
			if err != nil {
				fmt.Printf("\nerror %s, retry...\n", err.Error())
				time.Sleep(500 * time.Millisecond)
				continue
			}
			break
		}
		if err != nil {
			return nil, err
		}
		// Process logs
		progress := 1 - float64(blockNow-fromBlock.Int64())/float64(blockNow-startBlk)
		fmt.Printf("\rprocessing %d swap events: %.2f", len(logs), progress)
		for _, vLog := range logs {
			var event SwapEvent

			err := swapABI.UnpackIntoInterface(&event, "Swap", vLog.Data)
			if err != nil {
				log.Printf("\nfailed to unpack log data: %v\n", err)
				continue
			}
			sym := v3.PoolAddrToSymbol[vLog.Address.Hex()]
			px := SqrtPriceX96ToPrice(event.SqrtPriceX96)

			prices[vLog.BlockNumber-uint64(startBlk)].symToPx[sym] = px
		}

		fromBlock = big.NewInt(toBlock.Int64() + 1)
	}
	log.Printf("\nfilterer processed.\n")
	return prices, nil
}

func (v3 *V3Client) getLogs(query ethereum.FilterQuery) ([]types.Log, error) {
	rec, err := v3.RpcHndl.WaitForRpc(TYPE_HTTPS, 15)
	defer v3.RpcHndl.ReturnLock(rec)
	if err != nil {
		return nil, err
	}
	client, err := ethclient.Dial(rec.Url)
	if err != nil {
		v3.RpcHndl.ReturnLock(rec)
		return nil, fmt.Errorf("error ethclient %s dial %v", rec.Url, err)
	}

	// Get logs
	logs, err := client.FilterLogs(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch logs with %s: %v", rec.Url, err)
	}
	return logs, nil
}

// Function to convert sqrtPriceX96 to price
func SqrtPriceX96ToPrice(sqrtPriceX96 *big.Int) float64 {
	// Constants
	sqrtPriceX96Shift := big.NewInt(1).Lsh(big.NewInt(1), 96)          // 2^96
	sqrtPrice := new(big.Float).SetInt(sqrtPriceX96)                   // Convert sqrtPriceX96 to big.Float
	sqrtPrice.Quo(sqrtPrice, new(big.Float).SetInt(sqrtPriceX96Shift)) // sqrtPriceX96 / 2^96

	// Square the value to get the price
	price := new(big.Float).Mul(sqrtPrice, sqrtPrice) // price = sqrtPrice^2
	px, _ := price.Float64()
	return px
}

func (v3 *V3Client) findStartingBlock(startTs uint64) (uint64, uint64, error) {
	var blk, blkNow, ts uint64
	var err error
	for trial := 0; trial < 3; trial++ {
		var rec Receipt
		rec, err = v3.RpcHndl.WaitForRpc(TYPE_HTTPS, 15)
		if err != nil {
			v3.RpcHndl.ReturnLock(rec)
			continue
		}
		client, err := ethclient.Dial(rec.Url)
		if err != nil {
			v3.RpcHndl.ReturnLock(rec)
			slog.Info("error ethclient dial", "error", err, "url", rec.Url)
			continue
		}
		blk, ts, blkNow, err = FindBlockWithTs(client, uint64(startTs))
		if err != nil {
			v3.RpcHndl.ReturnLock(rec)
			slog.Info("error finding block, retry", "error", err)
			continue
		}
		v3.RpcHndl.ReturnLock(rec)
		break
	}
	if err != nil {
		return 0, 0, err
	}
	fmt.Printf("found block with ts %d (searched for ts %d) diff=%d\n", ts, startTs, max(startTs, ts)-min(startTs, ts))
	return blk, blkNow, nil
}

// getEventSignatureHash calculates the event signature hash
func getEventSignatureHash(eventSig string) common.Hash {
	hash := sha3.NewLegacyKeccak256()
	hash.Write([]byte(eventSig))
	return common.BytesToHash(hash.Sum(nil))
}
