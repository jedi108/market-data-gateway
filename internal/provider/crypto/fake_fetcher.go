// Package crypto (local verification): an in-memory Fetcher so the gateway
// can serve crypto candles in -crypto-mode=fake without any vendor or network.
// This is the crypto analogue of the provider/fake package used for TBank.
package crypto

import (
	"context"
	"time"
)

// FakeFetcher returns a fixed set of candles per (venue, symbol, timeframe),
// aligned to candle boundaries. It is deterministic and used only for local
// smoke/verification; it never issues a real upstream call.
type FakeFetcher struct {
	// Candles maps "venue|symbol|timeframe" -> raw candles to return.
	Candles map[string][]RawCandle
	// Calls records how many FetchKlines invocations happened (verification).
	Calls int
}

// NewFakeFetcher builds a FakeFetcher seeded with one closed 1h candle per
// default venue/symbol so a freshly built gateway returns data immediately.
func NewFakeFetcher() *FakeFetcher {
	now := (time.Now().UnixMilli() / 3_600_000) * 3_600_000
	seed := []RawCandle{
		{OpenTimeMS: now - 3_600_000, Open: "100", High: "101", Low: "99", Close: "100.5", Volume: "12", IsClosed: true},
		{OpenTimeMS: now, Open: "100.5", High: "102", Low: "100", Close: "101", Volume: "13", IsClosed: true},
	}
	return &FakeFetcher{Candles: map[string][]RawCandle{
		"binance|BTCUSDT|1h": seed,
		"bybit|BTCUSDT|1h":   seed,
		"binance|ETHUSDT|1h": seed,
		"bybit|ETHUSDT|1h":   seed,
	}}
}

func (f *FakeFetcher) FetchKlines(_ context.Context, venue, symbol, timeframe string, fromMS, toMS int64, limit int) ([]RawCandle, error) {
	f.Calls++
	key := venue + "|" + symbol + "|" + timeframe
	all := f.Candles[key]
	out := make([]RawCandle, 0, len(all))
	for _, c := range all {
		if c.OpenTimeMS >= fromMS && c.OpenTimeMS < toMS {
			out = append(out, c)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
