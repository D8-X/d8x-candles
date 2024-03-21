package main

import (
	"d8x-candles/src/svc"
	"log/slog"
)

// Injected via -ldflags -X
var VERSION = "pyth-client-development"

func main() {
	slog.Info("starting service",
		slog.String("name", "pyth-client"),
		slog.String("version", VERSION),
	)
	svc.StreamPyth()
}
