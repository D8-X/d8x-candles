package config

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

//go:embed embedded/*
var EmbededConfigs embed.FS

type ConfigCandlePeriod struct {
	Period         string `json:"period"`
	TimeMs         int    `json:"timeMs"`
	DisplayRangeMs int    `json:"displayRangeMs"`
}

const (
	SYNC_HUB_URL = "https://raw.githubusercontent.com/D8-X/sync-hub/refs/heads/main/d8x-candles/"
)

func GetCandlePeriodsConfig() ([]ConfigCandlePeriod, error) {
	// Read the JSON file
	fs, err := EmbededConfigs.Open("embedded/candlePeriods.config.json")
	if err != nil {
		log.Fatal("Error reading JSON file:", err)
		return nil, err
	}
	data, err := io.ReadAll(fs)
	if err != nil {
		return nil, err
	}
	var conf []ConfigCandlePeriod
	err = json.Unmarshal(data, &conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

// FetchConfigFromRepo gets the config file from Github
func FetchConfigFromRepo(configName string) ([]byte, error) {
	url := SYNC_HUB_URL + configName
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error fetching config: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to fetch config, status code: %d", resp.StatusCode)
	}

	jsonData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading config: %v", err)
	}

	return jsonData, nil
}
