package service

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

type fakeProvider struct {
	mu       sync.Mutex
	calls    int
	requests []model.CandleRequest
	response func(model.CandleRequest) ([]model.Candle, error)
	started  chan struct{}
	release  chan struct{}
}

func (f *fakeProvider) FetchCandles(ctx context.Context, request model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	f.mu.Lock()
	f.calls++
	f.requests = append(f.requests, request)
	started, release := f.started, f.release
	f.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, model.ProviderMetadata{}, ctx.Err()
		}
	}
	candles, err := f.response(request)
	return candles, model.ProviderMetadata{Status: "fake"}, err
}

func newService(t *testing.T, path string, upstream *fakeProvider) (*Service, *storage.Store, *ratelimit.Scheduler) {
	t.Helper()
	store, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 100, WorkerCount: 2, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	svc, err := New(cache.New(), store, upstream, scheduler, Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return svc, store, scheduler
}

func closeService(t *testing.T, store *storage.Store, scheduler *ratelimit.Scheduler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := scheduler.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func request(from, to int64) model.CandleRequest {
	return model.CandleRequest{Series: model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: "1m", CandleType: "trade"}, FromUTCMS: from, ToUTCMS: to, Limit: 10}
}
func candlesFor(r model.CandleRequest) []model.Candle {
	var result []model.Candle
	for open := r.FromUTCMS; open < r.ToUTCMS; open += 60 {
		result = append(result, model.Candle{OpenTimeUTCMS: open, CloseTimeUTCMS: open + 60, Open: "1", High: "2", Low: "1", Close: "2", Volume: "1", IsClosed: true})
	}
	return result
}

func TestRequestPathColdOverlapSingleflightAndPersistentRestart(t *testing.T) {
	path := t.TempDir() + "/cache.sqlite"
	upstream := &fakeProvider{response: func(r model.CandleRequest) ([]model.Candle, error) { return candlesFor(r), nil }}
	svc, store, scheduler := newService(t, path, upstream)

	got, err := svc.Get(context.Background(), "first", ratelimit.PriorityLiveRefresh, request(0, 120))
	if err != nil || got.CacheStatus != CacheRefreshed || len(got.Candles) != 2 {
		t.Fatalf("cold result=%+v err=%v", got, err)
	}
	got, err = svc.Get(context.Background(), "first", ratelimit.PriorityLiveRefresh, request(60, 180))
	if err != nil || got.CacheStatus != CacheRefreshed || len(got.Candles) != 2 {
		t.Fatalf("overlap result=%+v err=%v", got, err)
	}
	upstream.mu.Lock()
	if upstream.calls != 2 || upstream.requests[1].FromUTCMS != 120 || upstream.requests[1].ToUTCMS != 180 {
		t.Fatalf("incremental requests=%+v calls=%d", upstream.requests, upstream.calls)
	}
	upstream.mu.Unlock()
	closeService(t, store, scheduler)

	upstream2 := &fakeProvider{response: func(model.CandleRequest) ([]model.Candle, error) { return nil, errors.New("should not fetch") }}
	svc, store, scheduler = newService(t, path, upstream2)
	defer closeService(t, store, scheduler)
	got, err = svc.Get(context.Background(), "restart", ratelimit.PriorityLiveRefresh, request(0, 120))
	if err != nil || got.CacheStatus != CachePersistentHit || len(got.Candles) != 2 {
		t.Fatalf("restart result=%+v err=%v", got, err)
	}
	if upstream2.calls != 0 {
		t.Fatalf("persistent restart called upstream %d times", upstream2.calls)
	}
}

func TestRequestPathCoalescesConcurrentMisses(t *testing.T) {
	upstream := &fakeProvider{started: make(chan struct{}, 1), release: make(chan struct{}), response: func(r model.CandleRequest) ([]model.Candle, error) { return candlesFor(r), nil }}
	svc, store, scheduler := newService(t, t.TempDir()+"/cache.sqlite", upstream)
	defer closeService(t, store, scheduler)
	var failures atomic.Int32
	var joins atomic.Int32
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := svc.Get(context.Background(), "concurrent", ratelimit.PriorityLiveRefresh, request(0, 120))
			if err != nil || len(got.Candles) != 2 {
				failures.Add(1)
			}
			if got.CacheStatus == CacheSingleflight {
				joins.Add(1)
			}
		}()
	}
	<-upstream.started
	deadline := time.Now().Add(time.Second)
	for svc.Metrics().SingleflightJoins == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(upstream.release)
	wg.Wait()
	if failures.Load() != 0 || upstream.calls != 1 || joins.Load() == 0 {
		t.Fatalf("failures=%d calls=%d joins=%d", failures.Load(), upstream.calls, joins.Load())
	}
}

func TestStaleIsReturnedOnlyForUpstreamFailure(t *testing.T) {
	path := t.TempDir() + "/cache.sqlite"
	seed := &fakeProvider{response: func(r model.CandleRequest) ([]model.Candle, error) { return candlesFor(r), nil }}
	svc, store, scheduler := newService(t, path, seed)
	if _, err := svc.Get(context.Background(), "seed", ratelimit.PriorityLiveRefresh, request(0, 120)); err != nil {
		t.Fatal(err)
	}
	closeService(t, store, scheduler)

	failed := &fakeProvider{response: func(model.CandleRequest) ([]model.Candle, error) { return nil, errors.New("upstream down") }}
	svc, store, scheduler = newService(t, path, failed)
	defer closeService(t, store, scheduler)
	got, err := svc.Get(context.Background(), "retry", ratelimit.PriorityLiveRefresh, request(0, 180))
	if err != nil || got.Freshness != StaleAcceptable || got.StaleCause == nil {
		t.Fatalf("stale result=%+v err=%v", got, err)
	}
	metrics := svc.Metrics()
	if metrics.StaleResponses != 1 {
		t.Fatalf("stale metric=%+v", metrics)
	}
}

// gapProvider skips the candle covering [60,120) — a model of a settled
// session gap (market closed) inside an otherwise tradable window.
func gapProvider() *fakeProvider {
	return &fakeProvider{response: func(r model.CandleRequest) ([]model.Candle, error) {
		var out []model.Candle
		for _, candle := range candlesFor(r) {
			if candle.OpenTimeUTCMS == 60 {
				continue
			}
			out = append(out, candle)
		}
		return out, nil
	}}
}

func TestRequestPathServesPartialCoverageWithSettledGaps(t *testing.T) {
	upstream := gapProvider()
	svc, store, scheduler := newService(t, t.TempDir()+"/cache.sqlite", upstream)
	defer closeService(t, store, scheduler)

	// First request: provider succeeds but the session gap keeps coverage
	// incomplete. The non-empty partial response is served, labeled stale —
	// never fresh.
	got, err := svc.Get(context.Background(), "partial", ratelimit.PriorityLiveRefresh, request(0, 180))
	if err != nil {
		t.Fatalf("Get error = %v, want partial coverage served", err)
	}
	if got.Freshness != StaleAcceptable || got.Freshness == Fresh || got.CacheStatus != CacheRefreshed {
		t.Fatalf("partial result=%+v", got)
	}
	if len(got.Candles) != 2 {
		t.Fatalf("partial candles=%d, want 2", len(got.Candles))
	}

	// Second request: the settled gap is fetched once, proven empty, and
	// marked as empty coverage; the window then completes.
	got, err = svc.Get(context.Background(), "partial", ratelimit.PriorityLiveRefresh, request(0, 180))
	if err != nil || got.Freshness != Fresh {
		t.Fatalf("post-gap result=%+v err=%v", got, err)
	}
	upstream.mu.Lock()
	calls := upstream.calls
	upstream.mu.Unlock()

	// Third request: fully covered — the empty gap is NOT re-fetched.
	if _, err = svc.Get(context.Background(), "partial", ratelimit.PriorityLiveRefresh, request(0, 180)); err != nil {
		t.Fatal(err)
	}
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	if upstream.calls != calls || calls != 2 {
		t.Fatalf("provider calls=%d, want the settled gap fetched exactly once (2)", upstream.calls)
	}
}

func TestRequestPathEmptyRangesAreNeverPersisted(t *testing.T) {
	path := t.TempDir() + "/cache.sqlite"
	upstream := gapProvider()
	svc, store, scheduler := newService(t, path, upstream)
	if _, err := svc.Get(context.Background(), "seed", ratelimit.PriorityLiveRefresh, request(0, 180)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(context.Background(), "seed", ratelimit.PriorityLiveRefresh, request(0, 180)); err != nil {
		t.Fatal(err)
	}
	closeService(t, store, scheduler)

	// Restart: memory-only empty marks are gone; SQLite keeps only real
	// candle spans, so the gap must be re-asked from the provider.
	restarted := gapProvider()
	svc, store, scheduler = newService(t, path, restarted)
	defer closeService(t, store, scheduler)
	if _, err := svc.Get(context.Background(), "restart", ratelimit.PriorityLiveRefresh, request(0, 180)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(context.Background(), "restart", ratelimit.PriorityLiveRefresh, request(0, 180)); err != nil {
		t.Fatal(err)
	}
	restarted.mu.Lock()
	defer restarted.mu.Unlock()
	if restarted.calls != 1 {
		t.Fatalf("provider calls=%d after restart, want exactly 1 (gap re-asked: absence is not persisted)", restarted.calls)
	}
}

func TestRequestPathDoesNotMarkUnsettledEmptyRanges(t *testing.T) {
	upstream := gapProvider()
	svc, store, scheduler := newService(t, t.TempDir()+"/cache.sqlite", upstream)
	defer closeService(t, store, scheduler)
	// "now" pinned just past the closed-bar boundary at 120000: the window
	// is trimmed to the closed portion [0,120000), the 60ms gap is NOT
	// settled (now - 2*step = 50ms), so empty responses stay retryable and
	// are never marked as coverage; the third poll hits the per-epoch range
	// cap and is served from cache (stale, honest).
	svc.now = func() time.Time { return time.UnixMilli(120050) }

	for i := 0; i < 3; i++ {
		got, err := svc.Get(context.Background(), "live", ratelimit.PriorityLiveRefresh, request(0, 180))
		if err != nil {
			t.Fatalf("Get error = %v, want partial coverage served", err)
		}
		if got.Freshness != StaleAcceptable {
			t.Fatalf("iteration %d: freshness=%s, want stale_acceptable", i, got.Freshness)
		}
	}
	upstream.mu.Lock()
	calls := upstream.calls
	upstream.mu.Unlock()
	// Bounded fetch attempts (cold window + two gap attempts); the per-epoch
	// range budget then defers further identical polls to cache (task 109).
	if calls != 3 {
		t.Fatalf("provider calls=%d, want the bounded 3 (cold + 2 gap attempts)", calls)
	}
	for i := 0; i < 3; i++ {
		if _, err := svc.Get(context.Background(), "live", ratelimit.PriorityLiveRefresh, request(0, 180)); err != nil {
			t.Fatal(err)
		}
	}
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	if upstream.calls != calls {
		t.Fatalf("deferred polls hit the provider: %d -> %d", calls, upstream.calls)
	}
}

func TestRequestPathMarksOnlySettledPrefixOfEmptyRange(t *testing.T) {
	const step int64 = 60_000
	key := request(0, step).Series
	upstream := &fakeProvider{response: func(model.CandleRequest) ([]model.Candle, error) {
		return nil, nil
	}}
	svc, store, scheduler := newService(t, t.TempDir()+"/cache.sqlite", upstream)
	defer closeService(t, store, scheduler)

	// The series already has real data up to the start of a long session gap.
	if err := svc.cache.MergeCoverage(key, []model.Candle{{
		OpenTimeUTCMS:  0,
		CloseTimeUTCMS: step,
		Open:           "1",
		High:           "2",
		Low:            "1",
		Close:          "2",
		Volume:         "1",
		IsClosed:       true,
	}}, []cache.Range{{FromUTCMS: 0, ToUTCMS: step}}, false); err != nil {
		t.Fatal(err)
	}

	// At the fifth bar boundary, the request reaches one forming bar. The
	// provider's empty response covers both a settled prefix and an unsettled
	// suffix of the missing range [step, 5*step).
	svc.now = func() time.Time { return time.UnixMilli(5 * step) }
	req := request(0, 6*step)
	got, err := svc.Get(context.Background(), "live-edge", ratelimit.PriorityLiveRefresh, req)
	if err != nil || got.Freshness != StaleAcceptable || len(got.Candles) != 1 {
		t.Fatalf("first result=%+v err=%v, want honest partial result", got, err)
	}

	lookup, err := svc.cache.Lookup(key, cache.Range{FromUTCMS: 0, ToUTCMS: 6 * step})
	if err != nil {
		t.Fatal(err)
	}
	if lookup.Complete || len(lookup.Missing) != 1 || lookup.Missing[0] != (cache.Range{FromUTCMS: 3 * step, ToUTCMS: 6 * step}) {
		t.Fatalf("coverage after crossing empty response=%+v, want only settled prefix [step,3*step)", lookup)
	}

	upstream.mu.Lock()
	if len(upstream.requests) != 1 || upstream.requests[0].FromUTCMS != step || upstream.requests[0].ToUTCMS != 5*step {
		t.Fatalf("first provider request=%+v, want [step,5*step)", upstream.requests)
	}
	upstream.mu.Unlock()

	// The next refresh must start at the unsettled suffix, never download the
	// already closed prefix again. The forming bar [5*step,6*step) is not
	// fetched or marked as coverage.
	if _, err := svc.Get(context.Background(), "live-edge", ratelimit.PriorityLiveRefresh, req); err != nil {
		t.Fatal(err)
	}
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	if len(upstream.requests) != 2 || upstream.requests[1].FromUTCMS != 3*step || upstream.requests[1].ToUTCMS != 5*step {
		t.Fatalf("follow-up provider requests=%+v, want suffix [3*step,5*step)", upstream.requests)
	}
}

func TestRequestPathRejectsSuccessfulButEmptyProviderCoverage(t *testing.T) {
	upstream := &fakeProvider{response: func(model.CandleRequest) ([]model.Candle, error) { return nil, nil }}
	svc, store, scheduler := newService(t, t.TempDir()+"/cache.sqlite", upstream)
	defer closeService(t, store, scheduler)

	got, err := svc.Get(context.Background(), "empty", ratelimit.PriorityLiveRefresh, request(0, 120))
	if !errors.Is(err, ErrIncompleteCoverage) {
		t.Fatalf("Get error = %v, want incomplete coverage", err)
	}
	var typed *apperror.Error
	if !errors.As(err, &typed) || typed.Code != apperror.CodeIncompleteCoverage || typed.HTTPStatus() != 503 {
		t.Fatalf("Get error is not typed incomplete coverage: %T %v", err, err)
	}
	if got.Freshness == Fresh || got.CacheStatus == CacheRefreshed {
		t.Fatalf("empty provider response was labeled successful: %+v", got)
	}
}

func TestClosedOnlyLiveResponseKeepsPresentationEdgeOutOfCoverage(t *testing.T) {
	const step int64 = 3_600_000
	boundary := (time.Now().UnixMilli() / step) * step
	key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: "1h", CandleType: "trade"}
	upstream := &fakeProvider{response: func(r model.CandleRequest) ([]model.Candle, error) {
		var candles []model.Candle
		for open := r.FromUTCMS; open < r.ToUTCMS; open += step {
			candles = append(candles, model.Candle{
				OpenTimeUTCMS: open, CloseTimeUTCMS: open + step,
				Open: "100", High: "101", Low: "99", Close: "100", Volume: "10", IsClosed: true,
			})
		}
		return candles, nil // T-Bank closed-only response: no forming candle.
	}}
	svc, store, scheduler := newService(t, t.TempDir()+"/closed-only.sqlite", upstream)
	defer closeService(t, store, scheduler)
	svc.now = func() time.Time { return time.UnixMilli(boundary + 123) }
	requested := cache.Range{FromUTCMS: boundary - 3*step, ToUTCMS: boundary + step}
	request := model.CandleRequest{Series: key, FromUTCMS: requested.FromUTCMS, ToUTCMS: requested.ToUTCMS, Limit: 10, IncludeIncomplete: true}

	got, err := svc.Get(context.Background(), "closed-only", ratelimit.PriorityLiveRefresh, request)
	if err != nil || got.Freshness != Fresh || len(got.Candles) != 3 {
		t.Fatalf("closed-only result=%+v err=%v, want three closed candles and fresh closed coverage", got, err)
	}
	for _, candle := range got.Candles {
		if !candle.IsClosed {
			t.Fatalf("provider response introduced forming candle into service result: %+v", got.Candles)
		}
	}

	lookup, err := svc.cache.Lookup(key, requested)
	if err != nil {
		t.Fatal(err)
	}
	if lookup.Complete || len(lookup.Missing) != 1 || lookup.Missing[0] != (cache.Range{FromUTCMS: boundary, ToUTCMS: boundary + step}) {
		t.Fatalf("memory coverage=%+v, want live edge missing and closed prefix covered", lookup)
	}
	snapshot, found, err := store.Get(key, requested.FromUTCMS, requested.ToUTCMS)
	wantCoverage := []storage.Interval{
		{FromUTCMS: boundary - 3*step, ToUTCMS: boundary - 2*step},
		{FromUTCMS: boundary - 2*step, ToUTCMS: boundary - step},
		{FromUTCMS: boundary - step, ToUTCMS: boundary},
	}
	if err != nil || !found || len(snapshot.Metadata.Coverage) != len(wantCoverage) || !reflect.DeepEqual(snapshot.Metadata.Coverage, wantCoverage) {
		t.Fatalf("durable snapshot found=%v coverage=%+v err=%v, want closed-only coverage", found, snapshot.Metadata.Coverage, err)
	}
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	if len(upstream.requests) != 1 || upstream.requests[0].FromUTCMS != boundary-3*step || upstream.requests[0].ToUTCMS != boundary {
		t.Fatalf("provider request=%+v, want closed portion only", upstream.requests)
	}
}
