package uniutils

import (
	"context"
	"d8x-candles/src/globalrpc"
	"errors"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/exp/slog"
)

// FindBlockWithTs attempts to find the block with given timestamp. Returns
// the block number found, timestamp of block found, current block, and potentially an error
func FindBlockWithTs(client *ethclient.Client, ts uint64) (uint64, uint64, uint64, error) {
	blockB, err := BlockByNumberL2Compat(client, context.Background(), nil)
	if err != nil {
		return 0, 0, 0, err
	}
	tsB := blockB.Time()
	numB := blockB.Number().Uint64()
	if ts >= tsB {
		return numB, tsB, numB, nil
	}
	blockNow := numB
	// guess and search so that ts is between tsA and tsB
	var numA, tsA, numCalls uint64
	var timeEst float64 = 3
	numCalls = 0
	for {
		tDiff := float64(tsB - ts)
		blocksBack := uint64(max(tDiff/timeEst, 1))
		if blocksBack >= numB {
			return 0, 0, blockNow, errors.New("genesis Block reached timestamp search failed")
		}
		numA = numB - blocksBack
		numABig := big.NewInt(int64(numA))
		blockA, err := BlockByNumberL2Compat(client, context.Background(), numABig)
		numCalls++
		if err != nil {
			return 0, 0, blockNow, errors.New("RPC issue in FindBlockFromTs:" + err.Error())
		}
		tsA = blockA.Time()
		numA = blockA.Number().Uint64()
		if tsA < ts {
			break
		}
		timeEst = float64(tsB-tsA) / float64(blocksBack)
		tsB = tsA
		numB = numA
	}
	blockNo, tsFound, numCalls2, err := binSearch(client, numA, numB, ts)
	numCalls = numCalls + numCalls2
	slog.Info("Num rpc calls FindBlockWithTs=" + strconv.Itoa(int(numCalls)))
	return blockNo, tsFound, blockNow, err
}

func binSearch(client *ethclient.Client, numA uint64, numB uint64, ts uint64) (uint64, uint64, uint64, error) {

	var tsP, numP, numCalls uint64
	numCalls = 0
	for {
		numP = (numA + numB) / 2
		numPBig := big.NewInt(int64(numP))
		blockP, err := BlockByNumberL2Compat(client, context.Background(), numPBig)
		numCalls++
		if err != nil {
			return 0, 0, numCalls, errors.New("RPC issue in FindBlockFromTs(search):" + err.Error())
		}
		tsP = blockP.Time()
		if tsP < ts {
			numA = numP
		} else {
			numB = numP
		}
		if numB <= numA+2 {
			break
		}

	}
	return numP, tsP, numCalls, nil
}

// blockTs gets the block timestamp of the given block number using
// several trials
func BlockTs(blockNum int64, rpcHndl *globalrpc.GlobalRpc) (uint64, error) {
	var rec globalrpc.Receipt
	var err error
	var client *ethclient.Client
	ctx := context.Background()
	for trial := 0; trial < 3; trial++ {
		rec, err = rpcHndl.GetAndLockRpc(globalrpc.TypeHTTPS, 15)
		defer rpcHndl.ReturnLock(rec)
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
