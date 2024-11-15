package utils

import (
	"d8x-candles/config"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	embed "github.com/D8-X/d8x-futures-go-sdk/config"
	"github.com/D8-X/d8x-futures-go-sdk/utils"
	d8xUtils "github.com/D8-X/d8x-futures-go-sdk/utils"
)

type SymbolManager struct {
	ConfigFile          ConfigFile
	PriceFeedIds        []d8xUtils.PriceFeedId
	PythIdToSym         map[string]string       //pyth id (0xabc..) to symbol (btc-usd)
	SymToPythOrigin     map[string]string       //symbol (btc-usd) to pyth origin ("Crypto.BTC/USD")
	CandlePeriodsMs     map[string]CandlePeriod //period 1m,5m,... to timeMs and displayRangeMs
	SymConstructionMutx *sync.Mutex             //mutex when data for a symbol is being constructed
}

// New initializes a new SymbolManager
func (sm *SymbolManager) New(fileName string) error {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &sm.ConfigFile)
	if err != nil {
		return err
	}
	if len(sm.ConfigFile.PythPriceEndpoints) == 0 {
		// legacy config
		sm.ConfigFile.PythPriceEndpoints = make([]string, len(sm.ConfigFile.ObsoleteWS))
		for k, ws := range sm.ConfigFile.ObsoleteWS {
			httpsAddr, _ := strings.CutPrefix(ws, "wss")
			httpsAddr, _ = strings.CutSuffix(httpsAddr, "/ws")
			httpsAddr = "https" + httpsAddr
			slog.Info(fmt.Sprintf("switching provided address %s to http-stream address %s", ws, httpsAddr))
			sm.ConfigFile.PythPriceEndpoints[k] = httpsAddr
		}
	}
	err = sm.extractPythIdToSymbolMap()
	if err != nil {
		return err
	}
	sm.SymConstructionMutx = &sync.Mutex{}
	err = sm.extractCandlePeriods()
	if err != nil {
		return err
	}
	return nil
}

func (c *SymbolManager) ExtractCCY(pxType PriceType) ([]string, error) {
	config, err := embed.GetDefaultPriceConfigByName("PythEVMStable")
	if err != nil {
		return nil, err
	}
	syms := make(map[string]bool)
	for _, item := range config.PriceFeedIds {
		if item.Type != pxType.ToString() {
			continue
		}
		ccys := strings.Split(item.Symbol, "-")
		syms[ccys[0]] = true
		syms[ccys[1]] = true
	}
	// store map into array
	res := make([]string, len(syms))
	i := 0
	for s := range syms {
		res[i] = s
		i++
	}
	return res, nil
}

// creates a map from ids "0x32121..." to symbols "xau-usd"
func (c *SymbolManager) extractPythIdToSymbolMap() error {
	slog.Info("Loading VAA ids for network PythEVMStable")
	config, err := embed.GetDefaultPriceConfigByName("PythEVMStable")
	if err != nil {
		return err
	}
	mIdToSym := make(map[string]string, len(config.PriceFeedIds))
	mSymToPythSym := make(map[string]string, len(config.PriceFeedIds))
	irrelevantSyms := make(map[string]struct{})
	for _, sym := range config.CandleIrrelevant {
		irrelevantSyms[strings.ToUpper(sym)] = struct{}{}
	}
	c.PriceFeedIds = make([]utils.PriceFeedId, 0, len(config.PriceFeedIds))
	for k, el := range config.PriceFeedIds {
		if el.Type != TYPE_PYTH.ToString() {
			continue
		}
		s := strings.ToUpper(el.Symbol)
		if _, ok := irrelevantSyms[s]; ok {
			continue
		}
		idTrim, _ := strings.CutPrefix(el.Id, "0x")
		mIdToSym[idTrim] = s
		mSymToPythSym[s] = el.Origin
		c.PriceFeedIds = append(c.PriceFeedIds, config.PriceFeedIds[k])
	}
	c.PythIdToSym = mIdToSym
	c.SymToPythOrigin = mSymToPythSym
	return nil
}

func (c *SymbolManager) extractCandlePeriods() error {

	periods, err := config.GetCandlePeriodsConfig()
	if err != nil {
		return err
	}
	c.CandlePeriodsMs = make(map[string]CandlePeriod, len(periods))
	for _, el := range periods {
		p := strings.ToUpper(el.Period)
		c.CandlePeriodsMs[p] = CandlePeriod{Name: p, TimeMs: el.TimeMs, DisplayRangeMs: el.DisplayRangeMs}
	}
	return nil
}
