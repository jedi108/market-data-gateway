package storage

import (
	"github.com/jedi108/market-data-gateway/internal/model"
	"testing"
)

func TestFundingSnapshotRestartAndUnknownRejection(t *testing.T) {
	path := t.TempDir() + "/gateway.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	key := model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true}
	event := model.FundingEvent{Ts: 100, Rate: "0.00100", IntervalH: 8, AvailableAt: 100, Availability: "historical_payment_assumption"}
	snap := Snapshot{FundingEvents: []model.FundingEvent{event}, Metadata: Metadata{Coverage: []Interval{{0, 200}}, SourceFetchedAtUTCMS: 200}}
	if err = s.Save(key, snap); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, found, err := s.Get(key, 0, 200)
	if err != nil || !found || len(got.FundingEvents) != 1 || got.FundingEvents[0] != event || len(got.Candles) != 0 || len(got.Metadata.Coverage) != 1 {
		t.Fatalf("%+v %t %v", got, found, err)
	}
	key.Completeness = "unknown"
	if err = s.Save(key, snap); err == nil {
		t.Fatal("unknown accepted")
	}
}
