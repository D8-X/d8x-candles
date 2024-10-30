package v3client

import (
	"fmt"
	"testing"
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
