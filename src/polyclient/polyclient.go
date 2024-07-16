package polyclient

import (
	"d8x-candles/src/utils"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

func GetMarketInfo(conditionId string) (*utils.PolyMarketInfo, error) {
	endpt := "https://clob.polymarket.com/markets/" + conditionId
	resp, err := http.Get(endpt)
	if err != nil {
		return nil, fmt.Errorf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var marketInfo utils.PolyMarketInfo
	err = json.Unmarshal(body, &marketInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	return &marketInfo, nil
}

func subscribe(c *websocket.Conn, assetIds []string) error {
	subscribeMessage := map[string]interface{}{
		"type":       "Market",
		"assets_ids": assetIds,
	}

	message, err := json.Marshal(subscribeMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal subscribe message: %w", err)
	}

	err = c.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		return fmt.Errorf("failed to write subscribe message: %w", err)
	}

	return nil
}

func StreamWs(assetIds []string) error {
	url := "wss://ws-subscriptions-clob.polymarket.com/ws/market"
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("dial: %s", err.Error())
	}
	defer c.Close()

	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			handleEvent(string(message))
		}
	}()

	err = subscribe(c, assetIds)
	if err != nil {
		return err
	}

	for {
		select {
		case err := <-errCh:
			fmt.Println("WebSocket connection error")
			return err
		case <-done:
			fmt.Println("WebSocket connection closed")
			return nil
		}
	}
}

func handleEvent(eventJson string) error {
	// Process price change event
	var eventMap map[string]interface{}
	err := json.Unmarshal([]byte(eventJson), &eventMap)
	if err != nil {
		return fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	eventType, ok := eventMap["event_type"].(string)
	if !ok {
		return fmt.Errorf("event_type not found or not a string: %s", eventJson)
	}
	slog.Info("received event " + eventType)
	if eventType == "book" {
		return nil
	}
	var priceChange utils.PolyPriceChange
	err = json.Unmarshal([]byte(eventJson), &priceChange)
	if err != nil {
		return fmt.Errorf("error unmarshalling price change JSON: %v", err)
	}
	fmt.Printf("asset=%s price = %s\n", priceChange.AssetID, priceChange.Price)
	restQueryPrice(priceChange.AssetID)
	return nil
}

func restQueryPrice(tokenId string) error {
	url := "https://clob.polymarket.com/midpoint?token_id=" + tokenId
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to make GET request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	var px utils.PolyMidPrice
	err = json.Unmarshal(body, &px)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %v", err)
	}
	fmt.Printf("mid price = %.2f\n", px.Px)
	return nil
}
