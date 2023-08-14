package init

import (
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

type PythHistoryAPI struct {
	BaseUrl string
}

type PythHistoryAPIResponse struct {
	S string    `json:"s"`
	T []uint32  `json:"t"`
	O []float64 `json:"o"`
	H []float64 `json:"h"`
	L []float64 `json:"l"`
	C []float64 `json:"c"`
	V []float64 `json:"v"`
}

/*
		1 -> 1 minute (works for 24h)
		2, 5, 15, 30, 60, 120, 240, 360, 720 -> min
	    1D, 1W, 1M
*/
func (p *PythHistoryAPI) RetrieveCandles(sym utils.SymbolPyth, candleRes utils.PythCandleResolution, fromTSSec uint32, toTsSec uint32) (PythHistoryAPIResponse, error) {
	endpoint := "/v1/shims/tradingview/history"
	query := "?symbol=" + sym.ToString() + "&resolution=" + candleRes.ToString() + "&from=" + strconv.Itoa(int(fromTSSec)) + "&to=" + strconv.Itoa(int(toTsSec))

	url := strings.TrimSuffix(p.BaseUrl, "/") + endpoint + query
	// Send a GET request
	response, err := http.Get(url)
	if err != nil {
		return PythHistoryAPIResponse{}, fmt.Errorf("Error making GET request: %v", err)
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
		return PythHistoryAPIResponse{}, fmt.Errorf("Error parsing GET request: %v", err)
	}

	return apiResponse, nil
}
