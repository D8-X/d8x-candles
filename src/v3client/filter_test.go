package v3client

import (
	"d8x-candles/env"
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestInterpolateTs(t *testing.T) {
	// type BlockObs struct {
	// 	blockNum int64
	// 	ts       uint64 //timestamp
	// 	symToPx  map[string]float64
	// }
	obs := make(map[uint64]*BlockObs)

	blocks := []uint64{100, 110, 120, 130, 140, 150}
	timest := []uint64{0, 0, 110, 0, 120, 0}
	for j := 0; j < len(blocks); j++ {
		obs[blocks[j]] = &BlockObs{
			ts:      timest[j],
			symToPx: make(map[string]float64),
		}
	}

	interpolateTs(obs)
	fmt.Println("block; ts")
	for j := 0; j < len(blocks); j++ {
		currobs := obs[blocks[j]]
		fmt.Printf("%d; %d\n", blocks[j], currobs.ts)
	}
}

func TestFilter(t *testing.T) {
	v := loadEnv()
	cUni := "../../config/v3_idx_conf.json"
	cRpc := "../../config/v3_rpc_conf.json"
	v3, err := NewV3Client(cUni, cRpc, v.GetString(env.REDIS_ADDR), v.GetString(env.REDIS_PW))
	if err != nil {
		fmt.Printf("error %v", err)
		t.FailNow()
	}
	nowTs := time.Now().Unix()
	start := nowTs - LOOKBACK_SEC
	blk, blkNow, err := v3.findStartingBlock(uint64(start))
	if err != nil {
		t.FailNow()
	}
	prices, err := v3.runFilterer(int64(blk), int64(blkNow))
	if err != nil {
		t.FailNow()
	}
	v3.findBlockTs(prices)
	// now triangulate available prices
	v3.fillTriangulatedHistory(prices)
	// check prices
	blocks := make([]uint64, len(prices))
	j := 0
	for block := range prices {
		blocks[j] = block
		j++
	}
	slices.Sort(blocks)
	lastTs := uint64(0)
	for j, block := range blocks {
		p := prices[block]
		if p.ts < lastTs {
			fmt.Printf("wrong order of timestamp at index %d\n", j)
			t.FailNow()
		}
		p.ts = lastTs
	}
}
