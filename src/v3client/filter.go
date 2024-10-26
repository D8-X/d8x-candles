package v3client

import (
	"context"
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

func (v3 *V3Client) Filter() error {
	nowTs := time.Now().Unix()
	start := nowTs - 86400*1
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
	//...
	// finally insert to redis
	//...
	return nil
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
				fmt.Printf("error %s, retry...\n", err.Error())
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
		fmt.Printf("processing %d swap events: %.2f\n", len(logs), progress)
		for _, vLog := range logs {
			var event SwapEvent

			err := swapABI.UnpackIntoInterface(&event, "Swap", vLog.Data)
			if err != nil {
				log.Printf("Failed to unpack log data: %v", err)
				continue
			}
			sym := v3.PoolAddrToSymbol[vLog.Address.Hex()]
			px := SqrtPriceX96ToPrice(event.SqrtPriceX96)

			prices[vLog.BlockNumber-uint64(startBlk)].symToPx[sym] = px
		}

		fromBlock = big.NewInt(toBlock.Int64() + 1)
	}

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
			continue
		}
		client, err := ethclient.Dial(rec.Url)
		if err != nil {
			slog.Info("error ethclient dial", "error", err, "url", rec.Url)
			continue
		}
		blk, ts, blkNow, err = FindBlockWithTs(client, uint64(startTs))
		if err != nil {
			slog.Info("error finding block, retry", "error", err)
			continue
		}
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
