// Package crypto implements the crypto Provider for the Market Data Gateway.
//
// Design boundary:
//   - The Provider implements provider.Provider (same interface as the TBank
//     adapter), so it slots into the existing singleflight/cache/routing
//     pipeline unchanged.
//   - All venue-specific wire logic lives behind the Fetcher interface. The
//     concrete Fetcher is CCXT-style (REST klines) but is injected, so unit
//     tests run against a fake Fetcher with zero network and zero vendor code.
//   - Candle semantics (timeframe vocabulary, publication lag, closed-only)
//     are taken from the Registry, never from the TBank constants.
//   - Decimal prices/volume are kept as strings (big.Rat validated) exactly
//     like the TBank provider; conversion to float happens only at the client
//     adapter boundary.
package crypto

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// Fetcher is the venue-specific klines boundary. A concrete implementation
// wraps CCXT (or a direct REST client); tests inject a fake.
//
// Candle is the normalized raw kline returned by the venue before decimal
// string coercion. OpenTimeMS must be the candle OPEN epoch ms in UTC.
type Fetcher interface {
	// FetchKlines returns raw candles for the given venue-native symbol and
	// timeframe between [fromMS, toMS). It returns an empty slice (not an
	// error) when the venue has no data in the window.
	FetchKlines(ctx context.Context, venue, symbol, timeframe string, fromMS, toMS int64, limit int) ([]RawCandle, error)
}

// RawCandle is the venue-agnostic raw kline shape.
type RawCandle struct {
	OpenTimeMS int64
	Open       string
	High       string
	Low        string
	Close      string
	Volume     string
	// IsClosed reports whether the candle is finalized. Venues that only ever
	// return closed candles may set this true unconditionally.
	IsClosed bool
}

// Provider is the crypto provider behind the provider.Provider interface.
type Provider struct {
	fetcher  Fetcher
	registry *Registry
}

// New builds a crypto Provider.
func New(fetcher Fetcher, registry *Registry) *Provider {
	return &Provider{fetcher: fetcher, registry: registry}
}

// FetchCandles implements provider.Provider.
func (p *Provider) FetchCandles(ctx context.Context, request model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	if err := request.Series.Validate(); err != nil {
		return nil, model.ProviderMetadata{}, apperror.New(apperror.CodeInvalidRequest, err.Error(), 0)
	}
	venue := request.Series.Venue
	if venue != "binance" && venue != "bybit" {
		return nil, model.ProviderMetadata{}, apperror.New(apperror.CodeInvalidRequest, "crypto provider only serves binance/bybit", 0)
	}
	semantics := p.registry.Semantics(venue)

	// Reject unsupported timeframes at the provider boundary too (defense in
	// depth; the resolver already filtered). This keeps the provider safe even
	// when invoked directly (e.g. peer owner).
	if !semantics.AllowedTimeframes[request.Series.Timeframe] {
		return nil, model.ProviderMetadata{}, apperror.New(apperror.CodeInvalidRequest,
			fmt.Sprintf("unsupported crypto timeframe %q", request.Series.Timeframe), 0)
	}

	raw, err := p.fetcher.FetchKlines(ctx, venue, request.Series.ProviderInstrumentID, request.Series.Timeframe, request.FromUTCMS, request.ToUTCMS, request.Limit)
	if err != nil {
		return nil, model.ProviderMetadata{}, mapFetcherError(err)
	}

	candles := make([]model.Candle, 0, len(raw))
	for _, r := range raw {
		// Closed-only venues / include_incomplete=false: drop forming candle.
		if !request.IncludeIncomplete && (!r.IsClosed || semantics.ClosedOnly) {
			continue
		}
		step := stepMS(request.Series.Timeframe)
		candle := model.Candle{
			OpenTimeUTCMS:  r.OpenTimeMS,
			CloseTimeUTCMS: r.OpenTimeMS + step,
			Open:           r.Open,
			High:           r.High,
			Low:            r.Low,
			Close:          r.Close,
			Volume:         r.Volume,
			IsClosed:       r.IsClosed,
		}
		if err := candle.Validate(); err != nil {
			// A malformed raw candle must not poison the whole batch; surface
			// as a provider-unavailable integrity rejection.
			return nil, model.ProviderMetadata{}, apperror.New(apperror.CodeProviderUnavailable,
				fmt.Sprintf("invalid crypto candle: %v", err), 0)
		}
		candles = append(candles, candle)
	}
	return candles, model.ProviderMetadata{Status: "ok"}, nil
}

// stepMS returns the candle duration in ms for a timeframe. It is independent
// of TBank's interval mapping; crypto uses the same UTC alignment as Freqtrade.
func stepMS(timeframe string) int64 {
	units := map[string]int64{"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
	unit := timeframe[len(timeframe)-1:]
	val, err := strconv.Atoi(timeframe[:len(timeframe)-1])
	if err != nil {
		return 60_000
	}
	if u, ok := units[unit]; ok {
		return u * int64(val)
	}
	return 60_000
}

func mapFetcherError(err error) error {
	var typed *apperror.Error
	if asAppError(err, &typed) {
		return typed
	}
	return apperror.New(apperror.CodeProviderUnavailable, fmt.Sprintf("crypto fetcher: %v", err), 0)
}

// asAppError mirrors errors.As for *apperror.Error without importing errors
// at every call site.
func asAppError(err error, target **apperror.Error) bool {
	if e, ok := err.(*apperror.Error); ok {
		*target = e
		return true
	}
	return false
}

// PublicationLag is exposed for the freshness policy to use per venue.
func (p *Provider) PublicationLag(venue string) time.Duration {
	return time.Duration(p.registry.Semantics(venue).PublicationLagMS) * time.Millisecond
}
