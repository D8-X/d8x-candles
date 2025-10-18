package sportsclient

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/D8-X/d8x-futures-go-sdk/pkg/d8x_futures"
	"github.com/gorilla/websocket"
	"github.com/redis/rueidis"
)

type SportsClient struct {
	Ruedi        rueidis.Client
	WsUrl        string
	Conn         *websocket.Conn
	SdkRO        *d8x_futures.SdkRO
	KnownSymbols *Window
}

func NewSports(WsUrl, RedisAddr, RedisPw string, chainId int) (*SportsClient, error) {
	sp := SportsClient{
		WsUrl:        WsUrl,
		KnownSymbols: NewWindow(86400),
	}
	var err error
	sp.Ruedi, err = rueidis.NewClient(
		rueidis.ClientOption{InitAddress: []string{RedisAddr}, Password: RedisPw})
	if err != nil {
		return nil, fmt.Errorf("failed to init Redis: %w", err)
	}
	sp.SdkRO, err = d8x_futures.NewSdkRO(strconv.Itoa(chainId))
	if err != nil {
		return nil, err
	}
	return &sp, nil
}

func (sp *SportsClient) Run() error {
	slog.Info("sports oracle")
	ctx := context.Background()
	for {
		timeStart := time.Now()
		err := sp.listenWs(ctx)
		if err != nil {
			slog.Info(fmt.Sprintf("Reconnecting WS, after %s reason: %v\n", time.Since(timeStart), err))
			sleep := max(1, 10-time.Since(timeStart))
			time.Sleep(sleep * time.Second) // Reconnect after a delay
		} else {
			fmt.Println("Connection closed gracefully")
			return nil
		}
	}
}
