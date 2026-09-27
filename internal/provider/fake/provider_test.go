package fake

import (
	"context"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/model"
)

func key(timeframe string) model.SeriesKey {
	return model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi-test", Timeframe: timeframe, CandleType: "trade"}
}

func TestFakeProviderGeneratesAlignedDeterministicCandles(t *testing.T) {
	provider := New()
	// 3-minute window with a 500-candle limit: generation is bounded by the
	// interval, then the limit in a separate case below.
	request := model.CandleRequest{Series: key("1m"), FromUTCMS: 1_000, ToUTCMS: 181_000, Limit: 500}
	first, metadata, err := provider.FetchCandles(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 {
		t.Fatalf("candles=%d want 3", len(first))
	}
	if first[0].OpenTimeUTCMS != 0 {
		t.Fatalf("first open=%d want grid-aligned 0 (1_000 aligns down to the 0 boundary)", first[0].OpenTimeUTCMS)
	}
	if metadata.Status != "fake" {
		t.Fatalf("metadata=%+v", metadata)
	}
	second, _, err := provider.FetchCandles(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != len(first) || second[0] != first[0] || second[len(second)-1] != first[len(first)-1] {
		t.Fatal("fake provider output is not deterministic")
	}
	for index, candle := range first {
		if err := candle.Validate(); err != nil {
			t.Fatalf("candle %d invalid: %v", index, err)
		}
		if candle.OpenTimeUTCMS%60_000 != 0 {
			t.Fatalf("candle %d misaligned: %d", index, candle.OpenTimeUTCMS)
		}
	}
	limited, _, err := provider.FetchCandles(context.Background(), model.CandleRequest{Series: key("1m"), FromUTCMS: 0, ToUTCMS: 600_000, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 {
		t.Fatalf("limited candles=%d want 2", len(limited))
	}
	if provider.Calls() != 3 {
		t.Fatalf("calls=%d", provider.Calls())
	}
}

func TestFakeProviderRejectsInvalidInterval(t *testing.T) {
	provider := New()
	if _, _, err := provider.FetchCandles(context.Background(), model.CandleRequest{Series: key("1m"), FromUTCMS: 100, ToUTCMS: 100, Limit: 1}); err == nil {
		t.Fatal("expected interval rejection")
	}
	invalid := key("1m")
	invalid.Venue = ""
	if _, _, err := provider.FetchCandles(context.Background(), model.CandleRequest{Series: invalid, FromUTCMS: 0, ToUTCMS: 60, Limit: 1}); err == nil {
		t.Fatal("expected series rejection")
	}
}
