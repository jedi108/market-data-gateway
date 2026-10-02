package model

import (
	"fmt"
	"math/big"
	"strings"
)

// SeriesKey is the stored series identity. ProviderInstrumentID is always the
// PHYSICAL provider instrument identifier (a share FIGI or a physical futures
// contract instrument_uid) — never a logical alias such as "GOLD". Two
// expiries of the same logical future therefore carry different UIDs and are
// distinct series end to end: distinct cache entries, distinct SQLite series
// rows, distinct coverage. Logical aliases exist only for request resolution
// and never participate in Identity().
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

// Instrument identity/unit vocabulary (F20 futures support). The physical
// identity of a series is its provider instrument UID; the fields below are
// response metadata only and never widen SeriesKey or its storage identity.
const (
	// InstrumentTypeShare is a cash-market instrument (TBank shares).
	InstrumentTypeShare = "share"
	// InstrumentTypeFuture is a physical-delivery futures contract. Its UID is
	// unique per expiry; price quotations are in points, not currency.
	InstrumentTypeFuture = "future"

	// PriceUnitPoints is the futures price domain: quotations are points.
	PriceUnitPoints = "points"
	// PriceUnitCurrency is the share price domain: quotations are in the
	// instrument's trading currency (see InstrumentMetadata.Currency).
	PriceUnitCurrency = "currency"

	// VolumeUnitLots matches the TBank HistoricCandle contract: volume is
	// reported in lots for both shares and futures.
	VolumeUnitLots = "lots"

	// ProviderIDKindFIGI identifies a share by its FIGI.
	ProviderIDKindFIGI = "figi"
	// ProviderIDKindInstrumentUID identifies a physical futures contract by
	// its instrument_uid.
	ProviderIDKindInstrumentUID = "instrument_uid"
)

// InstrumentMetadata carries the physical-instrument identity and unit
// contract behind a series so a downstream consumer never has to reconstruct
// the physical contract (or its units) from a ticker string. It is attached to
// gateway responses as additive metadata; share responses gain it without any
// change to their series identity or candles.
type InstrumentMetadata struct {
	// InstrumentType is InstrumentTypeShare or InstrumentTypeFuture.
	InstrumentType string `json:"instrument_type"`
	// CanonicalSymbol is the registry's canonical spelling of the instrument
	// (for futures, the physical contract code such as "GOLD-26.12"), not the
	// logical rolling alias.
	CanonicalSymbol string `json:"canonical_symbol,omitempty"`
	// ProviderIDKind reports how ProviderInstrumentID of the series key must
	// be interpreted: ProviderIDKindFIGI or ProviderIDKindInstrumentUID.
	ProviderIDKind string `json:"provider_id_kind,omitempty"`
	// PriceUnit is the price domain of OHLC values: PriceUnitPoints for
	// futures, PriceUnitCurrency for shares.
	PriceUnit string `json:"price_unit,omitempty"`
	// VolumeUnit is the unit of candle volume (VolumeUnitLots).
	VolumeUnit string `json:"volume_unit,omitempty"`
	// Currency names the quote currency for PriceUnitCurrency instruments
	// (shares); empty for futures (points are not a currency).
	Currency string `json:"currency,omitempty"`
	// ExpirationUTCMS is the physical futures contract expiration in UTC ms
	// since epoch. Zero for instruments without expiration (shares).
	ExpirationUTCMS int64 `json:"expiration_utc_ms,omitempty"`
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
