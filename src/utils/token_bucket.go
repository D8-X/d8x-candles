package utils

import (
	"log/slog"
	"sync"
	"time"
)

/* USAGE
capacity := 4     // the bucket can hold at most 4 tokens
refillRate := 4.0 // Tokens per second added
tb := utils.NewTokenBucket(capacity, refillRate)
tb.WaitForToken(250 * time.Millisecond, "poly api access")
*/

type TokenBucket struct {
	tokens      int
	capacity    int
	refillRate  float64 // Tokens per second
	lastRefill  time.Time
	refillMutex sync.Mutex
}

func NewTokenBucket(capacity int, refillRate float64) *TokenBucket {
	return &TokenBucket{
		tokens:     capacity,
		capacity:   capacity,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tokensToAdd := int(elapsed * tb.refillRate)
	if tokensToAdd > 0 {
		tb.tokens = min(tb.capacity, tb.tokens+tokensToAdd)
		tb.lastRefill = now
	}
}

// WaitForToken returns if a token is available, otherwise it waits
// and retries to get a token with the specified interval. If log is not "",
// displays a log message when waiting
func (tb *TokenBucket) WaitForToken(waitInterval time.Duration, log string) {
	for {
		if tb.Take() {
			return
		}
		if log != "" {
			slog.Info("too many requests, slowing down for " + log)
		}
		time.Sleep(waitInterval)
	}
}

func (tb *TokenBucket) Take() bool {
	tb.refillMutex.Lock()
	defer tb.refillMutex.Unlock()

	tb.refill()

	if tb.tokens > 0 {
		tb.tokens--
		return true
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
