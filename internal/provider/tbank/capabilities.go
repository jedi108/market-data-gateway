// Explicit capability boundary of the TBank gateway boundary (F20).
//
// The gateway is a candles-only transport/cache layer. The ONLY upstream input
// it serves is historical candles via MarketDataService.GetCandles, whose
// instrument_id accepts both a share FIGI and a physical futures contract
// instrument_uid — the same RPC therefore serves futures without any order or
// trading capability being added.
//
// Everything else a TBank account can do stays with direct TBank consumers and
// must never be proxied through the gateway silently:
//   - orderbooks (GetOrderBook / orderbook streams),
//   - trade and quote streams (MarketStreamService),
//   - last prices and last trades,
//   - instrument metadata services (InstrumentsService lookups),
//   - any order/trading or sandbox RPC.
//
// Silent fallback between sources is forbidden: an input outside this boundary
// fails with the typed UNSUPPORTED_CAPABILITY error, never an empty response
// and never a redirect to another source.

package tbank

import (
	"fmt"

	"github.com/jedi108/market-data-gateway/internal/apperror"
)

// CapabilityCandles is the historical-candles capability — the only input the
// gateway serves from the TBank market-data surface.
const CapabilityCandles = "candles"

// supportedCapabilities is the fixed served set. Extending it is a deliberate
// boundary change, not a runtime configuration knob.
var supportedCapabilities = map[string]bool{
	CapabilityCandles: true,
}

// SupportedCapabilities returns the served capability vocabulary in stable
// order (for startup logs and documentation).
func SupportedCapabilities() []string {
	return []string{CapabilityCandles}
}

// CheckCapability fails closed for any capability outside the served set. It
// is the single enforcement point shared by the registry (instrument classes)
// and any future request-surface gate; callers must surface the typed error
// instead of returning empty data.
func CheckCapability(capability string) error {
	if !supportedCapabilities[capability] {
		return apperror.New(apperror.CodeUnsupportedCapability, fmt.Sprintf("gateway does not serve capability %q; use a direct TBank client", capability), 0)
	}
	return nil
}
