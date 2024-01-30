package config

import (
	"embed"
	"encoding/json"
	"io"
	"log"
)

//go:embed embedded/*
var EmbededConfigs embed.FS

type ConfigCandlePeriod struct {
	Period         string `json:"period"`
	TimeMs         int    `json:"timeMs"`
	DisplayRangeMs int    `json:"displayRangeMs"`
}

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
