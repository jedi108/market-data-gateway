package service

import (
	"context"
	"errors"
	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/storage"
	"strings"
	"testing"
	"time"
)

type fundingProvider struct{ calls int }

func (p *fundingProvider) FetchCandles(context.Context, model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	panic("funding must not use candles")
}
func (p *fundingProvider) FetchFunding(ctx context.Context, r model.CandleRequest) ([]model.FundingEvent, error) {
	if err := ratelimit.RequireScheduledAttempt(ctx); err != nil {
		return nil, err
	}
	p.calls++
	return []model.FundingEvent{{Ts: r.FromUTCMS, Rate: "0.001", IntervalH: 8, AvailableAt: r.FromUTCMS, Availability: "historical_payment_assumption"}}, nil
}
func TestFundingReconciledServiceCacheAndRestart(t *testing.T) {
	path := t.TempDir() + "/cache.db"
	source := &fundingProvider{}
	open := func() (*Service, *storage.Store, *ratelimit.Scheduler) {
		store, err := storage.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 100, WorkerCount: 2, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
		svc, err := New(cache.New(), store, source, scheduler, Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		return svc, store, scheduler
	}
	req := model.CandleRequest{Series: model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true}, FromUTCMS: 100, ToUTCMS: 200, Limit: 10}
	svc, store, scheduler := open()
	got, err := svc.Get(context.Background(), "test", ratelimit.PriorityLiveRefresh, req)
	if err != nil || len(got.FundingEvents) != 1 || got.CacheStatus != CacheRefreshed {
		t.Fatalf("first %+v %v", got, err)
	}
	var metrics strings.Builder
	svc.recorder().Snapshot().WritePrometheus(&metrics)
	if !strings.Contains(metrics.String(), `endpoint="get_funding"`) {
		t.Fatalf("missing funding upstream metric: %s", metrics.String())
	}
	closeService(t, store, scheduler)
	svc, store, scheduler = open()
	defer closeService(t, store, scheduler)
	got, err = svc.Get(context.Background(), "test", ratelimit.PriorityLiveRefresh, req)
	if err != nil || len(got.FundingEvents) != 1 || got.CacheStatus != CachePersistentHit || source.calls != 1 {
		t.Fatalf("restart %+v %v calls=%d", got, err, source.calls)
	}
}

func TestFundingUnavailableProviderNeverUsesPartialSnapshot(t *testing.T) {
	path := t.TempDir() + "/cache.db"
	source := &fundingProvider{}
	store, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 100, WorkerCount: 2, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	defer closeService(t, store, scheduler)
	svc, err := New(cache.New(), store, source, scheduler, Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	key := model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true}
	event := model.FundingEvent{Ts: 100, Rate: "0.001", IntervalH: 8, AvailableAt: 100, Availability: "historical_payment_assumption"}
	if err := store.Save(key, storage.Snapshot{FundingEvents: []model.FundingEvent{event}, Metadata: storage.Metadata{Coverage: []storage.Interval{{FromUTCMS: 100, ToUTCMS: 150}}, SourceFetchedAtUTCMS: time.Now().UnixMilli()}}); err != nil {
		t.Fatal(err)
	}
	// An unavailable provider must not turn a partly covered window into a stale success.
	svc.provider = &fakeProvider{response: func(model.CandleRequest) ([]model.Candle, error) { panic("unexpected candle request") }}
	_, err = svc.Get(context.Background(), "test", ratelimit.PriorityLiveRefresh, model.CandleRequest{Series: key, FromUTCMS: 100, ToUTCMS: 200, Limit: 10})
	var typed *apperror.Error
	if !errors.As(err, &typed) || typed.Code != apperror.CodeCoverageUnknown || typed.Retryable {
		t.Fatalf("expected non-retryable coverage unknown, got %v", err)
	}
}

func TestFundingCachedResultOverLimitFailsClosed(t *testing.T) {
	path := t.TempDir() + "/cache.db"
	source := &fundingProvider{}
	store, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 100, WorkerCount: 2, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	defer closeService(t, store, scheduler)
	svc, err := New(cache.New(), store, source, scheduler, Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	key := model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true}
	events := []model.FundingEvent{{Ts: 100, Rate: "0.001", IntervalH: 8, AvailableAt: 100, Availability: "historical_payment_assumption"}, {Ts: 150, Rate: "0.002", IntervalH: 8, AvailableAt: 150, Availability: "historical_payment_assumption"}}
	if err := store.Save(key, storage.Snapshot{FundingEvents: events, Metadata: storage.Metadata{Coverage: []storage.Interval{{FromUTCMS: 100, ToUTCMS: 200}}}}); err != nil {
		t.Fatal(err)
	}
	_, err = svc.Get(context.Background(), "test", ratelimit.PriorityLiveRefresh, model.CandleRequest{Series: key, FromUTCMS: 100, ToUTCMS: 200, Limit: 1})
	var typed *apperror.Error
	if !errors.As(err, &typed) || typed.Code != apperror.CodeCoverageUnknown {
		t.Fatalf("expected coverage error, got %v", err)
	}
	if source.calls != 0 {
		t.Fatal("cached limit rejection reached provider")
	}
}

func TestFundingCachedEmptySubwindowFailsClosed(t *testing.T) {
	store, err := storage.Open(t.TempDir() + "/cache.db")
	if err != nil {
		t.Fatal(err)
	}
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 100, WorkerCount: 2, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	defer closeService(t, store, scheduler)
	source := &fundingProvider{}
	svc, err := New(cache.New(), store, source, scheduler, Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	key := model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true}
	event := model.FundingEvent{Ts: 100, Rate: "0.001", IntervalH: 8, AvailableAt: 100, Availability: "historical_payment_assumption"}
	if err := store.Save(key, storage.Snapshot{FundingEvents: []model.FundingEvent{event}, Metadata: storage.Metadata{Coverage: []storage.Interval{{FromUTCMS: 100, ToUTCMS: 200}}}}); err != nil {
		t.Fatal(err)
	}
	full := model.CandleRequest{Series: key, FromUTCMS: 100, ToUTCMS: 200, Limit: 10}
	if _, err := svc.Get(context.Background(), "test", ratelimit.PriorityLiveRefresh, full); err != nil {
		t.Fatal(err)
	}
	request := model.CandleRequest{Series: key, FromUTCMS: 150, ToUTCMS: 200, Limit: 10}
	for i := 0; i < 2; i++ {
		if result, err := svc.Get(context.Background(), "test", ratelimit.PriorityLiveRefresh, request); err != nil {
			var typed *apperror.Error
			if !errors.As(err, &typed) || typed.Code != apperror.CodeCoverageUnknown {
				t.Fatalf("expected coverage error, got %v", err)
			}
		} else {
			t.Fatalf("empty cached subwindow accepted: %+v", result)
		}
	}
	if source.calls != 0 {
		t.Fatal("cached rejection reached provider")
	}
}

func TestFundingPeerOwnerFailsClosed(t *testing.T) {
	svc := &Service{}
	req := model.CandleRequest{Series: model.SeriesKey{CandleType: "funding"}}
	if _, err := svc.GetOwned(req); err == nil {
		t.Fatal("peer funding silently loses events")
	}
	items := svc.GetOwnedBatch([]model.CandleRequest{req})
	if len(items) != 1 || items[0].Error == nil {
		t.Fatal("peer batch funding silently loses events")
	}
}
