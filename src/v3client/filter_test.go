package v3client

import (
	"d8x-candles/env"
	"d8x-candles/src/uniutils"
	"d8x-candles/src/utils"
	"fmt"
	"testing"
)

func TestFilter(t *testing.T) {
	v := loadEnv()
	cUni := "../../config/v3_idx_conf.json"
	cRpc := "../../config/v3_rpc_conf.json"
	v3, err := NewV3Client(cUni, cRpc, v.GetString(env.REDIS_ADDR), v.GetString(env.REDIS_PW))
	if err != nil {
		fmt.Printf("error %v", err)
		t.FailNow()
	}
	eventSigHash := uniutils.GetEventSignatureHash(SWAP_EVENT_SIGNATURE)
	fltr, err := uniutils.NewFilter(
		utils.TYPE_V3,
		v3.Config.Indices,
		v3.RpcHndl,
		SWAP_EVENT_ABI,
		eventSigHash,
		v3.handleSwapEvent,
	)
	if err != nil {
		fmt.Printf("error %v", err)
		t.FailNow()
	}
	err = fltr.Run(v3.Ruedi, v3.RelevantPoolAddrs)
	if err != nil {
		fmt.Printf("error %v", err)
		t.FailNow()
	}
}
