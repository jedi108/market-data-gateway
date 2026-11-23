package main

import (
	"testing"

	"github.com/jedi108/market-data-gateway/internal/model"
)

func TestParseDiagnosticSeriesKey(t *testing.T) {
	for _, key := range []model.SeriesKey{
		{Venue: "binance", MarketType: "spot", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "trade"},
		{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true},
		{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "mark", Completeness: "unknown", DiagnosticOnly: true},
	} {
		got, err := parseSeriesKey(key.Identity())
		if err != nil || got != key {
			t.Fatalf("round trip: got %+v, error %v, want %+v", got, err, key)
		}
	}
	for _, identity := range []string{"binance|futures|BTCUSDT|8h|funding", "binance|futures|BTCUSDT|1h|mark", "binance|futures|BTCUSDT|8h|funding|unknown|false", "binance|futures|BTCUSDT|8h|funding||true", "binance|futures|BTCUSDT|8h|trade|unknown|true"} {
		if _, err := parseSeriesKey(identity); err == nil {
			t.Fatalf("accepted %q", identity)
		}
	}
}
