package v3client

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestInterpolateTs(t *testing.T) {
	// type BlockObs struct {
	// 	blockNum int64
	// 	ts       uint64 //timestamp
	// 	symToPx  map[string]float64
	// }
	r := rand.New(rand.NewSource(42))
	obs := make([]BlockObs, 100)
	for k := range obs {
		obs[k].blockNum = int64(k)
	}
	for k := 0; k < len(obs); {
		obs[k].ts = uint64(k * 2)
		k = k + r.Intn(10)
	}
	interpolateTs(obs)
	for k := range obs {
		fmt.Printf("%d; %d\n", obs[k].blockNum, obs[k].ts)
	}
}
