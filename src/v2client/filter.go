package v2client

import (
	"d8x-candles/src/uniutils"
	"log/slog"

	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
	"github.com/ethereum/go-ethereum/core/types"
)

func (v2 *V2Client) Filter() error {
	eventSigHash := uniutils.GetEventSignatureHash(SYNC_EVENT_SIGNATURE)
	fltr, err := uniutils.NewFilter(
		d8xUtils.PXTYPE_V2,
		v2.Config.Indices,
		v2.ConfigPyth,
		v2.RpcHndl,
		SYNC_EVENT_ABI,
		eventSigHash,
		v2.handleSyncEvent,
	)
	if err != nil {
		return err
	}
	return fltr.Run(v2.Ruedi, v2.RelevantPoolAddrs)
}

// handleSyncEvent is the handler function passed to the uniutils.Filter
func (v2 *V2Client) handleSyncEvent(fltr *uniutils.Filter, vLog types.Log) {
	var event SyncEvent
	err := fltr.Abi.UnpackIntoInterface(&event, "Sync", vLog.Data)
	if err != nil {
		slog.Error("failed to unpack Sync log data", "error", err)
		return
	}
	info := v2.PoolAddrToInfo[vLog.Address.Hex()]
	r0 := uniutils.DecNToFloat(event.Reserve0, info.TokenDec[0])
	r1 := uniutils.DecNToFloat(event.Reserve1, info.TokenDec[1])
	px := r1 / r0
	if _, exists := fltr.Prices[vLog.BlockNumber]; !exists {
		fltr.Prices[vLog.BlockNumber] = &uniutils.BlockObs{
			Ts:      0, // unknown at this point
			SymToPx: make(map[string]float64),
		}
	}
	fltr.Prices[vLog.BlockNumber].SymToPx[info.Symbol] = px
}
