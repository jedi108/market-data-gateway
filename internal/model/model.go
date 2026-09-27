package model

import (
	"fmt"
	"math/big"
	"strings"
)

type SeriesKey struct {
	Venue, MarketType, ProviderInstrumentID, Timeframe, CandleType string
}

func (k SeriesKey) Validate() error {
	for name, value := range map[string]string{"venue": k.Venue, "market_type": k.MarketType, "provider_instrument_id": k.ProviderInstrumentID, "timeframe": k.Timeframe, "candle_type": k.CandleType} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("series key %s is required", name)
		}
	}
	return nil
}

func (k SeriesKey) Identity() string {
	return strings.Join([]string{k.Venue, k.MarketType, k.ProviderInstrumentID, k.Timeframe, k.CandleType}, "|")
}

type Candle struct {
	OpenTimeUTCMS, CloseTimeUTCMS  int64
	Open, High, Low, Close, Volume string
	IsClosed                       bool
}

func (c Candle) Validate() error {
	if c.OpenTimeUTCMS < 0 || c.CloseTimeUTCMS <= c.OpenTimeUTCMS {
		return fmt.Errorf("invalid candle interval")
	}
	values := make([]*big.Rat, 5)
	for i, value := range []string{c.Open, c.High, c.Low, c.Close, c.Volume} {
		parsed, ok := new(big.Rat).SetString(value)
		if !ok {
			return fmt.Errorf("invalid decimal at index %d", i)
		}
		values[i] = parsed
	}
	if values[2].Cmp(values[1]) > 0 || values[0].Cmp(values[1]) > 0 || values[3].Cmp(values[1]) > 0 || values[0].Cmp(values[2]) < 0 || values[3].Cmp(values[2]) < 0 || values[4].Sign() < 0 {
		return fmt.Errorf("invalid OHLCV invariants")
	}
	return nil
}

type CandleRequest struct {
	Series             SeriesKey
	FromUTCMS, ToUTCMS int64
	Limit              int
	IncludeIncomplete  bool
}
type ProviderMetadata struct {
	Status             string
	RateLimitRemaining int64
	RateLimitResetMS   int64
}
