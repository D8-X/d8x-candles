package v3client

import (
	"d8x-candles/src/uniutils"
	"d8x-candles/src/utils"
	"log"
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
)

func (v3 *V3Client) Filter() error {
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
		return err
	}
	return fltr.Run(v3.Ruedi, v3.RelevantPoolAddrs)
}

func (v3 *V3Client) handleSwapEvent(fltr *uniutils.Filter, vLog types.Log) {
	var event SwapEvent
	err := fltr.Abi.UnpackIntoInterface(&event, "Swap", vLog.Data)
	if err != nil {
		log.Printf("\nfailed to unpack log data: %v\n", err)
		return
	}
	sym := v3.PoolAddrToSymbol[vLog.Address.Hex()]
	px := SqrtPriceX96ToPrice(event.SqrtPriceX96)
	if _, exists := fltr.Prices[vLog.BlockNumber]; !exists {
		fltr.Prices[vLog.BlockNumber] = &uniutils.BlockObs{
			Ts:      0, // unknown at this point
			SymToPx: make(map[string]float64),
		}
	}
	fltr.Prices[vLog.BlockNumber].SymToPx[sym] = px
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
