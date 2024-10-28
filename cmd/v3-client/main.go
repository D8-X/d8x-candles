package main

import (
	"d8x-candles/src/svc"
	"log/slog"
)

// Injected via -ldflags -X
var VERSION = "v3-client-development"

func main() {
	slog.Info("starting service",
		slog.String("name", "v3-client"),
		slog.String("version", VERSION),
	)
	svc.RunV3Client()
}
