// Package crypto defines the crypto venue extension for the Market Data
// Gateway.
//
// It deliberately does NOT reuse TBank policy:
//   - TBank resolution is by FIGI + a versioned symbol registry and uses
//     Russian-trading-session candle semantics; crypto venues resolve by the
//     canonical symbol string (which contains '/', e.g. BTC/USDT) and use
//     24x7 candle alignment with NO session calendar.
//   - Candle semantics differ per venue: Binance and Bybit expose different
//     timeframe vocabularies and publication lags; these are encoded per venue
//     in CandleSemantics rather than imported from the TBank constants.
//
// The resolver maps a client (venue, symbol, timeframe, market_type,
// candle_type) tuple onto the canonical SeriesKey and returns the venue's
// candle semantics. Unknown venue/symbol/timeframe fail BEFORE any provider
// call (spec: ambiguous/unknown alias completes before provider call).
package crypto

import (
	"fmt"
	"strings"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// Instrument is a crypto trading pair recognized by the gateway for a venue.
// ProviderInstrumentID is the venue-native symbol (e.g. "BTCUSDT" for Binance
// spot, "BTC/USDT:USDT" for Bybit linear perpetuals). CanonicalSymbol is the
// human symbol used by clients (always contains '/').
type Instrument struct {
	Venue                string
	MarketType           string // "spot" | "futures"
	CandleType           string // "trade" | "mark" | "index"
	CanonicalSymbol      string // "BTC/USDT"
	ProviderInstrumentID string
	Aliases              []string
}

// CandleSemantics captures the per-venue, per-timeframe candle behaviour that
// the gateway must know to compute freshness and refuse unsupported windows.
// It is intentionally distinct from TBank's session-aware policy.
type CandleSemantics struct {
	// AllowedTimeframes is the venue-native timeframe vocabulary.
	AllowedTimeframes map[string]bool
	// PublicationLagMS is how long after a candle boundary the venue is
	// expected to have published the closed candle. Used for freshness.
	PublicationLagMS int64
	// IncompleteCandleSupported reports whether the venue exposes the forming
	// (not-yet-closed) candle. Binance klines do; Bybit v5 kline does too but
	// with a different 'confirm' flag semantics.
	IncompleteCandleSupported bool
	// ClosedOnly reports whether the gateway should drop the forming candle for
	// this venue when include_incomplete=false.
	ClosedOnly bool
}

// Registry resolves crypto symbols and holds per-venue candle semantics.
type Registry struct {
	instruments map[string]Instrument // canonical + alias -> instrument (per venue+market)
	semantics   map[string]CandleSemantics
}

// NewRegistry builds a crypto registry from the provided instruments.
// Duplicate canonical/alias symbols within the same (venue, market_type,
// candle_type) are rejected (spec: alias collision is an explicit error).
func NewRegistry(instruments []Instrument, semantics map[string]CandleSemantics) (*Registry, error) {
	r := &Registry{
		instruments: map[string]Instrument{},
		semantics:   semantics,
	}
	for _, ins := range instruments {
		if ins.Venue == "" || ins.CanonicalSymbol == "" || ins.ProviderInstrumentID == "" {
			return nil, fmt.Errorf("crypto instrument requires venue, canonical symbol and provider id")
		}
		if ins.MarketType == "" {
			ins.MarketType = "spot"
		}
		if ins.CandleType == "" {
			ins.CandleType = "trade"
		}
		key := indexKey(ins.Venue, ins.MarketType, ins.CandleType, ins.CanonicalSymbol)
		if _, dup := r.instruments[key]; dup {
			return nil, fmt.Errorf("duplicate crypto canonical symbol %q for %s/%s/%s", ins.CanonicalSymbol, ins.Venue, ins.MarketType, ins.CandleType)
		}
		r.instruments[key] = ins
		for _, alias := range ins.Aliases {
			akey := indexKey(ins.Venue, ins.MarketType, ins.CandleType, alias)
			if _, dup := r.instruments[akey]; dup {
				return nil, fmt.Errorf("duplicate crypto alias %q for %s", alias, ins.Venue)
			}
			r.instruments[akey] = ins
		}
	}
	return r, nil
}

func indexKey(venue, marketType, candleType, symbol string) string {
	return strings.ToUpper(venue) + "\x00" + strings.ToLower(marketType) + "\x00" + strings.ToLower(candleType) + "\x00" + strings.ToUpper(symbol)
}

// Resolve maps (venue, market_type, candle_type, symbol) to an Instrument.
func (r *Registry) Resolve(venue, marketType, candleType, symbol string) (Instrument, error) {
	if marketType == "" {
		marketType = "spot"
	}
	if candleType == "" {
		candleType = "trade"
	}
	ins, ok := r.instruments[indexKey(venue, marketType, candleType, symbol)]
	if !ok {
		// Fallback: also try trade candle type for the same symbol so a client
		// that omits candle_type still resolves common spot pairs.
		ins, ok = r.instruments[indexKey(venue, marketType, "trade", symbol)]
	}
	if !ok {
		return Instrument{}, apperror.New(apperror.CodeUnknownSymbol, fmt.Sprintf("unknown crypto symbol %q for venue %s", symbol, venue), 0)
	}
	return ins, nil
}

// Semantics returns the per-venue candle semantics, falling back to a safe
// closed-only default if the venue was not configured.
func (r *Registry) Semantics(venue string) CandleSemantics {
	if s, ok := r.semantics[strings.ToLower(venue)]; ok {
		return s
	}
	return CandleSemantics{
		AllowedTimeframes:         map[string]bool{"1m": true, "5m": true, "15m": true, "1h": true, "4h": true, "1d": true},
		PublicationLagMS:          2_000,
		IncompleteCandleSupported: true,
		ClosedOnly:                false,
	}
}

// IsTimeframeAllowed reports whether the venue supports this timeframe.
func (r *Registry) IsTimeframeAllowed(venue, timeframe string) bool {
	return r.Semantics(venue).AllowedTimeframes[timeframe]
}

// ResolveKey fully validates and maps a client request onto a canonical
// SeriesKey for a crypto venue. It fails before any provider call.
func (r *Registry) ResolveKey(venue, symbol, timeframe, marketType, candleType string) (model.SeriesKey, error) {
	if venue != "binance" && venue != "bybit" {
		return model.SeriesKey{}, apperror.New(apperror.CodeInvalidRequest, "unsupported crypto venue", 0)
	}
	ins, err := r.Resolve(venue, marketType, candleType, symbol)
	if err != nil {
		return model.SeriesKey{}, err
	}
	if !r.IsTimeframeAllowed(venue, timeframe) {
		return model.SeriesKey{}, apperror.New(apperror.CodeInvalidRequest, fmt.Sprintf("unsupported timeframe %q for venue %s", timeframe, venue), 0)
	}
	return model.SeriesKey{
		Venue:                venue,
		MarketType:           ins.MarketType,
		ProviderInstrumentID: ins.ProviderInstrumentID,
		Timeframe:            timeframe,
		CandleType:           ins.CandleType,
	}, nil
}
