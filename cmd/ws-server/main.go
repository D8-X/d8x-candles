package main

import (
	"d8x-candles/src/svc"
	"log/slog"
)

// Injected via -ldflags -X
var VERSION = "ws-server-development"

func main() {
	slog.Info("starting service",
		slog.String("name", "candles-ws-server"),
		slog.String("version", VERSION),
	)
	svc.RunCandleCharts()
}
