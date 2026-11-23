package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDiagnosticSeriesIdentityAndMetadata(t *testing.T) {
	key := SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding"}
	legacy := key.Identity()
	key.Completeness, key.DiagnosticOnly = "reconciled", true
	if err := key.Validate(); err != nil {
		t.Fatal(err)
	}
	reconciled := key.Identity()
	key.Completeness = "unknown"
	if err := key.Validate(); err != nil {
		t.Fatal(err)
	}
	if reconciled == key.Identity() || legacy == reconciled {
		t.Fatal("series labels must distinguish identities")
	}
	data, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"completeness":"unknown"`) || !strings.Contains(string(data), `"diagnostic_only":true`) {
		t.Fatalf("missing labels: %s", data)
	}
	for _, invalid := range []SeriesKey{
		{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding"},
		{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "mark"},
		{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled"},
		{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", DiagnosticOnly: true},
		{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "trade", Completeness: "unknown", DiagnosticOnly: true},
		{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "complete", DiagnosticOnly: true},
	} {
		if invalid.Validate() == nil {
			t.Fatalf("accepted invalid labels: %+v", invalid)
		}
	}
}

func TestUnlabeledTradeSeriesRemainValid(t *testing.T) {
	for _, market := range []string{"spot", "futures"} {
		key := SeriesKey{Venue: "binance", MarketType: market, ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "trade"}
		if err := key.Validate(); err != nil {
			t.Fatalf("%s trade: %v", market, err)
		}
	}
}
