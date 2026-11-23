package cache

import (
	"github.com/jedi108/market-data-gateway/internal/model"
	"testing"
)

func TestFundingCoverageTypedAndDiagnostic(t *testing.T) {
	key := model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true}
	event := model.FundingEvent{Ts: 100, Rate: "-0.001", IntervalH: 8, AvailableAt: 100, Availability: "historical_payment_assumption"}
	c := New()
	if err := c.MergeFundingCoverage(key, []model.FundingEvent{event}, []Range{{0, 200}}, false); err != nil {
		t.Fatal(err)
	}
	got, err := c.Lookup(key, Range{0, 200})
	if err != nil || !got.Complete || len(got.Entry.FundingEvents) != 1 || got.Entry.FundingEvents[0] != event || len(got.Entry.Candles) != 0 {
		t.Fatalf("%+v %v", got, err)
	}
	got, err = c.Lookup(key, Range{101, 200})
	if err != nil || len(got.Entry.FundingEvents) != 0 {
		t.Fatalf("range: %+v %v", got, err)
	}
	key.Completeness = "unknown"
	if err := c.MergeFundingCoverage(key, []model.FundingEvent{event}, []Range{{0, 200}}, false); err == nil {
		t.Fatal("unknown accepted")
	}
}
