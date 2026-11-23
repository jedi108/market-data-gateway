package service

import (
	"context"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/storage"
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
	closeService(t, store, scheduler)
	svc, store, scheduler = open()
	defer closeService(t, store, scheduler)
	got, err = svc.Get(context.Background(), "test", ratelimit.PriorityLiveRefresh, req)
	if err != nil || len(got.FundingEvents) != 1 || got.CacheStatus != CachePersistentHit || source.calls != 1 {
		t.Fatalf("restart %+v %v calls=%d", got, err, source.calls)
	}
}

func TestFundingIncompleteWindowNeverUsesStaleSnapshot(t *testing.T) {
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
	if err == nil {
		t.Fatal("partial funding served after upstream failure")
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
	if err == nil {
		t.Fatal("over-limit funding accepted")
	}
	if source.calls != 0 {
		t.Fatal("cached limit rejection reached provider")
	}
}
