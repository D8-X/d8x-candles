package uniutils

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// BlockByHashL2Compat is L2 compatible version of ethclient.Client.BlockByHash. It
// retrieves block header information only. Transactions information is not
// parsed due to go-etheruem's L2 incompatibility. Use it when getting block
// info fails with "invalid transaction type" error.
func BlockByHashL2Compat(c *ethclient.Client, ctx context.Context, hash common.Hash) (*types.Block, error) {
	return getL2CompatibleBlockHeader(c, ctx, "eth_getBlockByHash", hash, true)
}

// BlockByNumberL2Compat is L2 compatible version of ethclient.Client.BlockByNumber. It
// retrieves block header information only. Transactions information is not
// parsed due to go-etheruem's L2 incompatibility. Use it when getting block
// info fails with "invalid transaction type" error.
func BlockByNumberL2Compat(c *ethclient.Client, ctx context.Context, number *big.Int) (*types.Block, error) {
	return getL2CompatibleBlockHeader(c, ctx, "eth_getBlockByNumber", toBlockNumArg(number), true)
}

// getL2CompatibleBlockHeader queries given c RPC endpoint for block header
// information. This method is compatible with L2 chains for retrieving block
// info, but it does not parse transaction information, therefore, only block
// header information is available in returned *types.Block.
func getL2CompatibleBlockHeader(c *ethclient.Client, ctx context.Context, method string, args ...any) (*types.Block, error) {
	var raw json.RawMessage
	err := c.Client().CallContext(ctx, &raw, method, args...)
	if err != nil {
		return nil, err
	}

	// Decode header only.
	var head *types.Header
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, err
	}
	// When the block is not found, the API returns JSON null.
	if head == nil {
		return nil, ethereum.NotFound
	}

	return types.NewBlockWithHeader(head), nil
}

func toBlockNumArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	if number.Sign() >= 0 {
		return hexutil.EncodeBig(number)
	}
	// It's negative.
	if number.IsInt64() {
		return rpc.BlockNumber(number.Int64()).String()
	}
	// It's negative and large, which is invalid.
	return fmt.Sprintf("<invalid %d>", number)
}
