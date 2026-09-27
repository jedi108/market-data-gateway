// Package fake is a deterministic, test-only provider used by local
// verification and the composition root's "fake" mode. It never performs
// network I/O and generates aligned synthetic candles for any valid request.
//
// It must never be configured in production; main refuses to start with it
// outside explicit -provider=fake runs.
package fake

import (
	"context"
	"fmt"
	"sync"

	"github.com/jedi108/market-data-gateway/internal/model"
)

// InstrumentRegistry is intentionally minimal: the fake provider accepts any
// well-formed SeriesKey because the canonical alias registry already rejected
// unknown symbols at the HTTP boundary.
type Provider struct {
	mu     sync.Mutex
	calls  int
	candle func(request model.CandleRequest, openTimeUTCMS int64) model.Candle
}

func New() *Provider {
	return &Provider{candle: DefaultCandle}
}

// DefaultCandle produces a valid OHLCV candle with deterministic decimals.
func DefaultCandle(request model.CandleRequest, openTimeUTCMS int64) model.Candle {
	return model.Candle{OpenTimeUTCMS: openTimeUTCMS, CloseTimeUTCMS: openTimeUTCMS + stepMS(request), Open: "100", High: "101", Low: "99", Close: "100", Volume: "10", IsClosed: true}
}

// WithCandleFactory overrides candle generation for tests.
func (p *Provider) WithCandleFactory(factory func(request model.CandleRequest, openTimeUTCMS int64) model.Candle) *Provider {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.candle = factory
	return p
}

// Calls reports the number of FetchCandles invocations (test assertion aid).
func (p *Provider) Calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *Provider) FetchCandles(ctx context.Context, request model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, model.ProviderMetadata{}, err
	}
	if err := request.Series.Validate(); err != nil {
		return nil, model.ProviderMetadata{}, err
	}
	if request.FromUTCMS < 0 || request.ToUTCMS <= request.FromUTCMS {
		return nil, model.ProviderMetadata{}, fmt.Errorf("invalid request interval")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	step := stepMS(request)
	candles := make([]model.Candle, 0, (request.ToUTCMS-request.FromUTCMS)/step+1)
	for open := alignDown(request.FromUTCMS, step); open+step <= request.ToUTCMS; open += step {
		candle := p.candle(request, open)
		candles = append(candles, candle)
		if int64(len(candles)) >= int64(request.Limit) {
			break
		}
	}
	return candles, model.ProviderMetadata{Status: "fake", RateLimitRemaining: 0}, nil
}

func stepMS(request model.CandleRequest) int64 {
	switch request.Series.Timeframe {
	case "1m":
		return 60_000
	case "5m":
		return 300_000
	case "15m":
		return 900_000
	case "1h":
		return 3_600_000
	case "1d":
		return 86_400_000
	default:
		return 60_000
	}
}

func alignDown(value, step int64) int64 {
	return value / step * step
}
