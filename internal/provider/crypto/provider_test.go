package crypto

import (
	"context"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/provider"
)

// fakeFetcher is a deterministic in-memory Fetcher for unit tests.
type fakeFetcher struct {
	calls   int
	candles []RawCandle
	err     error
	lastReq model.CandleRequest
}

func (f *fakeFetcher) FetchKlines(_ context.Context, _, _, _ string, fromMS, toMS int64, limit int) ([]RawCandle, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := make([]RawCandle, 0, len(f.candles))
	for _, c := range f.candles {
		if c.OpenTimeMS >= fromMS && c.OpenTimeMS < toMS {
			out = append(out, c)
		}
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	sem := map[string]CandleSemantics{
		"binance": {
			AllowedTimeframes:         map[string]bool{"1m": true, "5m": true, "1h": true, "1d": true},
			PublicationLagMS:          2_000,
			IncompleteCandleSupported: true,
			ClosedOnly:                false,
		},
		"bybit": {
			AllowedTimeframes:         map[string]bool{"1m": true, "5m": true, "1h": true, "1d": true},
			PublicationLagMS:          3_000,
			IncompleteCandleSupported: true,
			ClosedOnly:                false,
		},
	}
	reg, err := NewRegistry([]Instrument{
		{Venue: "binance", MarketType: "spot", CandleType: "trade", CanonicalSymbol: "BTC/USDT", ProviderInstrumentID: "BTCUSDT"},
		{Venue: "bybit", MarketType: "futures", CandleType: "trade", CanonicalSymbol: "BTC/USDT", ProviderInstrumentID: "BTCUSDT"},
	}, sem)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return reg
}

func TestResolveKeyCanonicalAndAlias(t *testing.T) {
	reg := testRegistry(t)
	key, err := reg.ResolveKey("binance", "BTC/USDT", "1h", "spot", "trade")
	if err != nil {
		t.Fatal(err)
	}
	if key.ProviderInstrumentID != "BTCUSDT" || key.Venue != "binance" || key.Timeframe != "1h" {
		t.Fatalf("unexpected key: %#v", key)
	}
	// Unknown symbol fails before any provider call.
	if _, err := reg.ResolveKey("binance", "NOPE/USDT", "1h", "", ""); err == nil {
		t.Fatal("expected unknown symbol error")
	}
	// Unsupported timeframe fails.
	if _, err := reg.ResolveKey("binance", "BTC/USDT", "3m", "", ""); err == nil {
		t.Fatal("expected unsupported timeframe error")
	}
	// Unsupported venue fails.
	if _, err := reg.ResolveKey("tbank", "SBER", "1h", "", ""); err == nil {
		t.Fatal("expected unsupported venue error")
	}
}

func TestProviderMapsCryptoCandles(t *testing.T) {
	reg := testRegistry(t)
	fetcher := &fakeFetcher{candles: []RawCandle{
		{OpenTimeMS: 1_700_000_000_000, Open: "100", High: "101", Low: "99", Close: "100", Volume: "10", IsClosed: true},
		{OpenTimeMS: 1_700_000_360_000, Open: "100", High: "102", Low: "98", Close: "101", Volume: "11", IsClosed: false},
	}}
	p := New(fetcher, reg)
	req := model.CandleRequest{
		Series:            model.SeriesKey{Venue: "binance", MarketType: "spot", ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "trade"},
		FromUTCMS:         1_700_000_000_000,
		ToUTCMS:           1_700_001_000_000,
		Limit:             10,
		IncludeIncomplete: false,
	}
	candles, _, err := p.FetchCandles(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// include_incomplete=false -> forming (IsClosed=false) candle dropped.
	if len(candles) != 1 {
		t.Fatalf("expected 1 closed candle, got %d", len(candles))
	}
	if candles[0].CloseTimeUTCMS != candles[0].OpenTimeUTCMS+3_600_000 {
		t.Fatalf("close_time alignment wrong: %#v", candles[0])
	}
}

func TestProviderKeepsIncompleteWhenRequested(t *testing.T) {
	reg := testRegistry(t)
	fetcher := &fakeFetcher{candles: []RawCandle{
		{OpenTimeMS: 1_700_000_360_000, Open: "100", High: "102", Low: "98", Close: "101", Volume: "11", IsClosed: false},
	}}
	p := New(fetcher, reg)
	req := model.CandleRequest{
		Series:            model.SeriesKey{Venue: "binance", MarketType: "spot", ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "trade"},
		FromUTCMS:         1_700_000_000_000,
		ToUTCMS:           1_700_001_000_000,
		Limit:             10,
		IncludeIncomplete: true,
	}
	candles, _, err := p.FetchCandles(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(candles) != 1 || candles[0].IsClosed {
		t.Fatalf("expected 1 incomplete candle, got %#v", candles)
	}
}

func TestProviderRejectsNonCryptoVenue(t *testing.T) {
	reg := testRegistry(t)
	p := New(&fakeFetcher{}, reg)
	req := model.CandleRequest{Series: model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "X", Timeframe: "1h", CandleType: "trade"}, FromUTCMS: 1, ToUTCMS: 2}
	_, _, err := p.FetchCandles(context.Background(), req)
	aerr, ok := err.(*apperror.Error)
	if !ok || aerr.Code != apperror.CodeInvalidRequest {
		t.Fatalf("expected typed invalid request, got %T %[1]v", err)
	}
}

func TestMuxRoutesByVenue(t *testing.T) {
	binanceFetcher := &fakeFetcher{candles: []RawCandle{{OpenTimeMS: 1_700_000_000_000, Open: "1", High: "1", Low: "1", Close: "1", Volume: "1", IsClosed: true}}}
	bybitFetcher := &fakeFetcher{candles: []RawCandle{{OpenTimeMS: 1_700_000_000_000, Open: "2", High: "2", Low: "2", Close: "2", Volume: "2", IsClosed: true}}}
	mux := provider.NewMux(map[string]provider.Provider{
		"binance": New(binanceFetcher, testRegistry(t)),
		"bybit":   New(bybitFetcher, testRegistry(t)),
	})
	// binance request -> binance fetcher only
	_, _, err := mux.FetchCandles(context.Background(), model.CandleRequest{
		Series:    model.SeriesKey{Venue: "binance", MarketType: "spot", ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "trade"},
		FromUTCMS: 1_700_000_000_000, ToUTCMS: 1_700_000_400_000, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if binanceFetcher.calls != 1 || bybitFetcher.calls != 0 {
		t.Fatalf("mux routed to wrong venue: binance=%d bybit=%d", binanceFetcher.calls, bybitFetcher.calls)
	}
	// unknown venue -> typed error, no fetcher call
	_, _, err = mux.FetchCandles(context.Background(), model.CandleRequest{
		Series:    model.SeriesKey{Venue: "kraken", MarketType: "spot", ProviderInstrumentID: "XBT", Timeframe: "1h", CandleType: "trade"},
		FromUTCMS: 1, ToUTCMS: 2,
	})
	aerr, ok := err.(*apperror.Error)
	if !ok || aerr.Code != apperror.CodeInvalidRequest {
		t.Fatalf("expected typed invalid request for unknown venue, got %T %[1]v", err)
	}
}
