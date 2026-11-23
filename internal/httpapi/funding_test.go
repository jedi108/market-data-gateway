package httpapi

import (
	"context"
	"encoding/json"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type reconciledFundingProvider struct{ calls atomic.Int32 }

func (*reconciledFundingProvider) FetchCandles(context.Context, model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	panic("unexpected candle fetch")
}
func (p *reconciledFundingProvider) FetchFunding(ctx context.Context, r model.CandleRequest) ([]model.FundingEvent, error) {
	if err := ratelimit.RequireScheduledAttempt(ctx); err != nil {
		return nil, err
	}
	p.calls.Add(1)
	return []model.FundingEvent{{Ts: r.FromUTCMS, Rate: "0.00100", IntervalH: 4, AvailableAt: r.FromUTCMS, Availability: "historical_payment_assumption"}, {Ts: r.FromUTCMS + 1, Rate: "0.00200", IntervalH: 8, AvailableAt: r.FromUTCMS + 1, Availability: "historical_payment_assumption"}}, nil
}
func TestGETReconciledFundingThroughScheduledCache(t *testing.T) {
	store, err := storage.Open(t.TempDir() + "/cache.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 100, PerClientQuotaPerMinute: 100, MaxTrackedClients: 10, QueueCapacity: 10, WorkerCount: 1, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := scheduler.Drain(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	source := &reconciledFundingProvider{}
	svc, err := service.New(cache.New(), store, source, scheduler, service.Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		return model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true}, nil
	}
	h, err := NewGatewayHandlerWithIdentity("test", func() bool { return true }, svc, resolve, 10, 10, 1, WithFundingResolver(resolve))
	if err != nil {
		t.Fatal(err)
	}
	base := "/v1/candles?venue=binance&symbol=BTC%2FUSDT%3AUSDT&timeframe=8h&from_utc_ms=100&to_utc_ms=200&limit=10&include_incomplete=false&candle_type=funding"
	for _, suffix := range []string{"&completeness=unknown&diagnostic_only=true", "&completeness=reconciled&diagnostic_only=false", ""} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, base+suffix, nil))
		if w.Code == http.StatusOK {
			t.Fatalf("invalid diagnostic accepted: %s", suffix)
		}
	}
	if source.calls.Load() != 0 {
		t.Fatal("unknown reached upstream")
	}
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, base+"&completeness=reconciled&diagnostic_only=true", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		var body candleEnvelope
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || len(body.FundingEvents) != 2 || body.FundingEvents[0].Rate != "0.00100" || body.FundingEvents[0].IntervalH != 4 || body.FundingEvents[0].AvailableAt != 100 || body.FundingEvents[0].Availability != "historical_payment_assumption" || body.FundingEvents[1].IntervalH != 8 || body.FundingEvents[1].AvailableAt != 101 || body.FundingEvents[1].Availability != "historical_payment_assumption" || body.Series.Completeness != "reconciled" || !body.Series.DiagnosticOnly {
			t.Fatalf("response %+v %v", body, err)
		}
	}
	if source.calls.Load() != 1 {
		t.Fatalf("upstream calls=%d", source.calls.Load())
	}
}
