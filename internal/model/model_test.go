package model

import "testing"

func TestSeriesKeyValidateAndIdentity(t *testing.T) {
	key := SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "BBG004730N88", Timeframe: "1h", CandleType: "trade"}
	if err := key.Validate(); err != nil {
		t.Fatalf("valid key: %v", err)
	}
	if got, want := key.Identity(), "tbank|shares|BBG004730N88|1h|trade"; got != want {
		t.Fatalf("Identity() = %q, want %q", got, want)
	}
}

func TestSeriesKeyRejectsMissingField(t *testing.T) {
	if err := (SeriesKey{Venue: "tbank"}).Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestCandleRejectsInvalidOHLC(t *testing.T) {
	candle := Candle{OpenTimeUTCMS: 1, CloseTimeUTCMS: 2, Open: "10", High: "9", Low: "8", Close: "9", Volume: "1", IsClosed: true}
	if err := candle.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}
