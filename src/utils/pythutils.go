package utils

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/rand"
)

type PythHistoryAPIResponse struct {
	S      string    `json:"s"`      // "ok", "error"
	T      []uint32  `json:"t"`      // bar time (ts seconds, start)
	O      []float64 `json:"o"`      // open
	H      []float64 `json:"h"`      // high
	L      []float64 `json:"l"`      // low
	C      []float64 `json:"c"`      // close
	V      []float64 `json:"v"`      // volume (empty)
	ErrMsg string    `json:"errmsg"` // "" or error
}

// GetPythHistory fetches candles from the pyth benchmark API
// The symbol is provided in the Pyth-Convention, e.g. Crypto.USDC/USD
func GetPythHistory(
	baseUrl string,
	tokenBkt *TokenBucket,
	pythSym string,
	candleRes PythCandleResolution,
	fromTSSec uint32,
	toTsSec uint32,
) (PythHistoryAPIResponse, error) {

	const endpoint = "/v1/shims/tradingview/history"
	query := "?symbol=" + pythSym + "&resolution=" + candleRes.ToPythString() + "&from=" + strconv.Itoa(int(fromTSSec)) + "&to=" + strconv.Itoa(int(toTsSec))

	url := strings.TrimSuffix(baseUrl, "/") + endpoint + query
	// Send a GET request
	var response *http.Response
	var err error
	for {
		if tokenBkt.Take() {
			response, err = http.Get(url)
			if err != nil {
				return PythHistoryAPIResponse{}, fmt.Errorf("error making GET request: %v", err)
			}
			break
		}
		slog.Info("too many requests, slowing down for " + pythSym)
		time.Sleep(time.Duration(50+rand.Intn(250)) * time.Millisecond)
	}

	defer response.Body.Close()

	// Check response status code
	if response.StatusCode != http.StatusOK {
		return PythHistoryAPIResponse{}, fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	// Read the response body
	var apiResponse PythHistoryAPIResponse
	err = json.NewDecoder(response.Body).Decode(&apiResponse)
	if err != nil {
		return PythHistoryAPIResponse{}, fmt.Errorf("error parsing GET request: %v", err)
	}
	if apiResponse.S == "error" {
		return PythHistoryAPIResponse{}, fmt.Errorf("error in benchmarks GET request: %s", apiResponse.ErrMsg)
	}

	return apiResponse, nil
}
