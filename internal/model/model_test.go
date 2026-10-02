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

// TestFuturesExpiriesAreDistinctIdentities pins the F20 storage identity
// contract: two physical expiries of one logical future differ by provider
// instrument UID, hence by series identity — cache entries never mix.
func TestFuturesExpiriesAreDistinctIdentities(t *testing.T) {
	december := SeriesKey{Venue: "tbank", MarketType: "futures", ProviderInstrumentID: "synthetic-uid-gold-2612", Timeframe: "1h", CandleType: "trade"}
	march := SeriesKey{Venue: "tbank", MarketType: "futures", ProviderInstrumentID: "synthetic-uid-gold-2703", Timeframe: "1h", CandleType: "trade"}
	if err := december.Validate(); err != nil {
		t.Fatal(err)
	}
	if december.Identity() == march.Identity() {
		t.Fatal("different expiries must yield different identities")
	}
	// Timeframe/schema-relevant component: same UID at another timeframe is
	// also a distinct series.
	otherTimeframe := december
	otherTimeframe.Timeframe = "1d"
	if otherTimeframe.Identity() == december.Identity() {
		t.Fatal("timeframe must participate in identity")
	}
}

// TestInstrumentMetadataUnitsAreExplicit pins the unit vocabulary so consumers
// read price domain and volume units from the response instead of inferring
// them from the ticker.
func TestInstrumentMetadataUnitsAreExplicit(t *testing.T) {
	future := InstrumentMetadata{InstrumentType: InstrumentTypeFuture, CanonicalSymbol: "GOLD-26.12", ProviderIDKind: ProviderIDKindInstrumentUID, PriceUnit: PriceUnitPoints, VolumeUnit: VolumeUnitLots, ExpirationUTCMS: 1797043200000}
	if future.PriceUnit != "points" || future.Currency != "" || future.VolumeUnit != "lots" {
		t.Fatalf("futures units: %+v", future)
	}
	share := InstrumentMetadata{InstrumentType: InstrumentTypeShare, CanonicalSymbol: "SBER", ProviderIDKind: ProviderIDKindFIGI, PriceUnit: PriceUnitCurrency, VolumeUnit: VolumeUnitLots, Currency: "RUB"}
	if share.PriceUnit != "currency" || share.Currency != "RUB" || share.ExpirationUTCMS != 0 {
		t.Fatalf("share units: %+v", share)
	}
}

func TestCandleRejectsInvalidOHLC(t *testing.T) {
	candle := Candle{OpenTimeUTCMS: 1, CloseTimeUTCMS: 2, Open: "10", High: "9", Low: "8", Close: "9", Volume: "1", IsClosed: true}
	if err := candle.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}
