package globalrpc

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

const EXPIRY_SEC = "60" // rpc locks expire after this amount of time
const REDIS_KEY_CURR_IDX = "glbl_rpc:idx"
const REDIS_KEY_URLS = "glbl_rpc:urls"
const REDIS_KEY_LOCK = "glbl_rpc:lock"
const REDIS_SET_URL_LOCK = "glbl_rpc:urls_lock"

type GlobalRpc struct {
	ruedi  *rueidis.Client
	Config RpcConfig
}

type Receipt struct {
	Url     string
	RpcType RPCType
}

// NewGlobalRpc initializes a new RPC handler for the given config-filename and
// redis credentials
func NewGlobalRpc(configname string, chainId int, redisAddr, redisPw string) (*GlobalRpc, error) {
	var gr GlobalRpc
	var err error
	gr.Config, err = loadRPCConfig(configname, chainId)
	if err != nil {
		return nil, err
	}
	// ruedis client
	client, err := rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{redisAddr}, Password: redisPw})
	if err != nil {
		return nil, err
	}
	// store URLs to Redis
	gr.ruedi = &client
	err = urlToRedis(gr.Config.ChainId, TypeHTTPS, gr.Config.Https, &client)
	if err != nil {
		return nil, err
	}
	err = urlToRedis(gr.Config.ChainId, TypeWSS, gr.Config.Wss, &client)
	if err != nil {
		return nil, err
	}
	return &gr, nil
}

// urlToRedis sets the urls for the given type and chain unless set recently
func urlToRedis(chain int, urlType RPCType, urls []string, client *rueidis.Client) error {
	c := *client
	// check whether anyone has set the urls recently
	key := REDIS_SET_URL_LOCK + strconv.Itoa(chain) + urlType.String()
	cmd := c.B().Set().Key(key).Value("locked").Nx().
		Ex(time.Minute * 10).Build()
	err := c.Do(context.Background(), cmd).Error()
	if err != nil {
		slog.Info("urls already set", "chain", chain, "type", urlType.String())
		return nil
	}
	// push urls
	key = REDIS_KEY_URLS + strconv.Itoa(chain) + "_" + urlType.String()
	cmd = c.B().Rpush().Key(key).Element(urls...).Build()
	if err := c.Do(context.Background(), cmd).Error(); err != nil {
		return err
	}
	return nil
}

// GetAndLockRpc returns a receipt
func (gr *GlobalRpc) GetAndLockRpc(rpcType RPCType, maxWaitSec int) (Receipt, error) {
	c := *gr.ruedi
	chainType := strconv.Itoa(gr.Config.ChainId) + "_" + rpcType.String()
	waitMs := 0

	var url string
	for {
		var err error
		cmd := c.B().Eval().Script(LUA_SCRIPT).Numkeys(3).Key(
			REDIS_KEY_CURR_IDX+chainType,
			REDIS_KEY_URLS+chainType,
			REDIS_KEY_LOCK+chainType).Arg(EXPIRY_SEC).Build()
		url, err = c.Do(context.Background(), cmd).ToString()
		if err != nil || url == "" {
			slog.Info("no available url", "chain_type", chainType)
			time.Sleep(250 * time.Millisecond)
			waitMs += 250
			if waitMs > maxWaitSec*1000 {
				return Receipt{}, fmt.Errorf("unable to get rpc")
			}
			continue
		}
		break
	}
	return Receipt{Url: url, RpcType: rpcType}, nil
}

// ReturnLock is for the user to return the receipt, so other
// instances can get the url. If the receipt is not returned,
// the lock expires after 60 seconds
func (gr *GlobalRpc) ReturnLock(rec Receipt) {
	chainType := strconv.Itoa(gr.Config.ChainId) + "_" + rec.RpcType.String()
	key := REDIS_KEY_LOCK + chainType + rec.Url
	c := *gr.ruedi
	cmd := c.B().Del().Key(key).Build()
	err := c.Do(context.Background(), cmd).Error()
	if err != nil {
		fmt.Println("error", err.Error())
	}
}
