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

func TestDiagnosticSeriesRejectsInverseAndNonUSDTProviderIDs(t *testing.T) {
	for _, candleType := range []string{"funding", "mark"} {
		for _, id := range []string{"BTCUSD_PERP", "BTCUSD", "BTCUSDC", "BTCBUSD", "USDT", "btcUSDT", "BTC/USDT:USDT"} {
			key := SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: id, Timeframe: "1h", CandleType: candleType, Completeness: "reconciled", DiagnosticOnly: true}
			if err := key.Validate(); err == nil {
				t.Errorf("accepted %s ID %q", candleType, id)
			}
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

func TestDiagnosticSeriesRequiresBinanceFutures(t *testing.T) {
	for _, candleType := range []string{"funding", "mark"} {
		for _, completeness := range []string{"reconciled", "unknown"} {
			for _, pair := range []struct {
				venue, market string
				valid         bool
			}{
				{"binance", "futures", true}, {"tbank", "futures", false},
				{"binance", "spot", false}, {"tbank", "spot", false},
			} {
				key := SeriesKey{Venue: pair.venue, MarketType: pair.market, ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: candleType, Completeness: completeness, DiagnosticOnly: true}
				if got := key.Validate() == nil; got != pair.valid {
					t.Errorf("%+v: valid=%v, want %v", key, got, pair.valid)
				}
			}
		}
	}
	for _, pair := range []struct{ venue, market string }{{"binance", "spot"}, {"tbank", "futures"}} {
		key := SeriesKey{Venue: pair.venue, MarketType: pair.market, ProviderInstrumentID: "BTCUSDT", Timeframe: "1m", CandleType: "trade"}
		if err := key.Validate(); err != nil {
			t.Errorf("trade key %+v: %v", key, err)
		}
	}
}

func TestSeriesKeyRejectsUnknownCandleType(t *testing.T) {
	key := SeriesKey{Venue: "binance", MarketType: "spot", ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "markk"}
	if err := key.Validate(); err == nil {
		t.Fatal("unknown candle type accepted")
	}
}
