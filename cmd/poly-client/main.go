package main

import (
	"d8x-candles/src/svc"
	"log/slog"
)

// Injected via -ldflags -X
var VERSION = "poly-client-development"

func main() {
	slog.Info("starting service",
		slog.String("name", "poly-client"),
		slog.String("version", VERSION),
	)
	svc.StreamPolyMarkets()
}
