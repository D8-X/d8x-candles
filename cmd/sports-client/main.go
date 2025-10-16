package main

import (
	"fmt"
	"log/slog"

	"d8x-candles/src/svc"
)

func main() {
	slog.Info("starting service",
		slog.String("name", "sport-client"),
	)
	if err := svc.StreamSport(); err != nil {
		fmt.Println(err)
		panic(err)
	}
}
