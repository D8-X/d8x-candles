package v3client

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const TYPE_WSS = 0
const TYPE_HTTPS = 1
const EXPIRY_SEC = 60 // rpc locks expire after this amount of time

type RpcHandler struct {
	Config     RpcConfig
	LockedTime [][]int64
	LastIdx    []int // we rotate RPC usage
	mtx        sync.Mutex
}

type Receipt struct {
	Url  string
	Type int
	Idx  int
}

func NewRpcHandler(filename string) (*RpcHandler, error) {
	var h RpcHandler
	var err error
	h.Config, err = LoadRPCConfig(filename)
	if err != nil {
		return nil, err
	}
	h.mtx = sync.Mutex{}
	h.LockedTime = make([][]int64, 2)
	h.LockedTime[TYPE_WSS] = make([]int64, len(h.Config.Wss))
	h.LockedTime[TYPE_HTTPS] = make([]int64, len(h.Config.Https))
	h.LastIdx = make([]int, 2)

	return &h, nil
}

// WaitForRpc waits for an available RPC up to maxWaitSec with
// 1 second delay between requests. Returns with an error if not successful
func (h *RpcHandler) WaitForRpc(rpcType, maxWaitSec int) (Receipt, error) {
	start := time.Now().Unix()
	for {
		rec, err := h.GetAndLockRpc(rpcType)
		if err == nil {
			return rec, nil
		}
		if time.Now().Unix()-start > int64(maxWaitSec) {
			return Receipt{}, fmt.Errorf("could not get rpc type %d", rpcType)
		}
		time.Sleep(time.Second)
	}
}

func (h *RpcHandler) GetAndLockRpc(rpcType int) (Receipt, error) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	now := time.Now().Unix()
	for j := 0; j < len(h.LockedTime[rpcType]); j++ {
		k := (h.LastIdx[rpcType] + 1 + j) % len(h.LockedTime[rpcType])
		if now-h.LockedTime[rpcType][k] > EXPIRY_SEC {
			// lock
			h.LockedTime[rpcType][k] = now
			h.LastIdx[rpcType] = k
			var url string
			if rpcType == TYPE_HTTPS {
				url = h.Config.Https[k]
			} else {
				url = h.Config.Wss[k]
			}
			return Receipt{Url: url, Type: rpcType, Idx: k}, nil
		}
	}
	return Receipt{}, errors.New("no availability")
}

func (h *RpcHandler) ReturnLock(rec Receipt) {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	// unlock
	h.LockedTime[rec.Type][rec.Idx] = 0
}
