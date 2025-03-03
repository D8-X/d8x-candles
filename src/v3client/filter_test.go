package v3client

import (
	"d8x-candles/env"
	"d8x-candles/src/uniutils"
	"fmt"
	"testing"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
)

func TestFilter(t *testing.T) {
	v := loadEnv()
	cRpc := "../../config/rpc_conf.json"
	chainId := v.GetInt(env.CHAIN_ID)
	conf := "../../config/v3_idx_conf.json"
	v3, err := NewV3Client(cRpc, v.GetString(env.REDIS_ADDR), v.GetString(env.REDIS_PW), chainId, conf)
	if err != nil {
		fmt.Printf("error %v", err)
		t.FailNow()
	}
	eventSigHash := uniutils.GetEventSignatureHash(SWAP_EVENT_SIGNATURE)
	fltr, err := uniutils.NewFilter(
		d8xUtils.PXTYPE_V3,
		v3.Config.Indices,
		v3.ConfigPyth,
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
