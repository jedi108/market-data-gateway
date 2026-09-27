package provider

import (
	"context"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// Mux routes a CandleRequest to the venue-specific provider. It is the
// single owner-selection point for upstream provider calls and keeps the
// service.Router (series ownership / peer routing) independent of venue
// identity.
//
// Only tbank and crypto (binance/bybit) are wired. An unknown venue is a
// typed 4xx at the provider boundary so it fails before any network call.
type Mux struct {
	byVenue map[string]Provider
}

// NewMux builds a Mux from a venue -> Provider map.
func NewMux(byVenue map[string]Provider) *Mux {
	return &Mux{byVenue: byVenue}
}

// Register adds or replaces a venue provider.
func (m *Mux) Register(venue string, p Provider) {
	if m.byVenue == nil {
		m.byVenue = map[string]Provider{}
	}
	m.byVenue[venue] = p
}

// FetchCandles implements Provider by delegating to the venue provider.
func (m *Mux) FetchCandles(ctx context.Context, request model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	p, ok := m.byVenue[request.Series.Venue]
	if !ok {
		return nil, model.ProviderMetadata{}, apperror.New(
			apperror.CodeInvalidRequest,
			"no provider configured for venue "+request.Series.Venue,
			0,
		)
	}
	return p.FetchCandles(ctx, request)
}
