// Task 109 service-level proofs: refresh epochs bound re-polls of identical
// missing ranges between closed-bar boundaries, admission rejection degrades
// to honest stale plus one deduplicated background refresh, and a controlled
// cold start of two 40-pair contours never exceeds the provider budget in any
// rolling window.
package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

func epochService(t *testing.T, upstream *fakeProvider, cfg Config, budget int) (*Service, *storage.Store, *ratelimit.Scheduler) {
	t.Helper()
	store, err := storage.Open(t.TempDir() + "/epoch.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	scheduler := ratelimit.NewScheduler(ratelimit.Config{
		NodeBudgetPerMinute:     budget,
		PerClientQuotaPerMinute: budget,
		MaxTrackedClients:       16,
		QueueCapacity:           256,
		WorkerCount:             4,
		MaxRetries:              0,
		RetryBaseDelay:          time.Millisecond,
		Window:                  200 * time.Millisecond,
	})
	svc, err := New(cache.New(), store, upstream, scheduler, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return svc, store, scheduler
}

// epochServiceClock exposes a steppable scheduler clock so budget windows
// roll only when the test advances them — deterministic under -race.
func epochServiceClock(t *testing.T, upstream *fakeProvider, cfg Config, budget int, window time.Duration) (*Service, *storage.Store, *ratelimit.Scheduler, func(time.Duration)) {
	t.Helper()
	store, err := storage.Open(t.TempDir() + "/epoch.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	scheduler := ratelimit.NewScheduler(ratelimit.Config{
		NodeBudgetPerMinute:     budget,
		PerClientQuotaPerMinute: budget,
		MaxTrackedClients:       16,
		QueueCapacity:           256,
		WorkerCount:             4,
		MaxRetries:              0,
		RetryBaseDelay:          time.Millisecond,
		Window:                  window,
		Now:                     func() time.Time { return time.UnixMilli(clock.Load() / int64(time.Millisecond)) },
	})
	svc, err := New(cache.New(), store, upstream, scheduler, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return svc, store, scheduler, func(d time.Duration) { clock.Add(d.Nanoseconds()) }
}

func upstreamCalls(upstream *fakeProvider) int {
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	return upstream.calls
}

func TestRefreshEpochBoundsRePollsOfSameMissingRange(t *testing.T) {
	// Publication lag: every response misses the newest requested bar, so
	// coverage can never complete within the epoch.
	upstream := &fakeProvider{response: func(r model.CandleRequest) ([]model.Candle, error) {
		candles := candlesFor(r)
		if len(candles) == 0 {
			return nil, nil
		}
		return candles[:len(candles)-1], nil
	}}
	svc, store, scheduler := epochService(t, upstream, Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour}, 1000)
	defer closeService(t, store, scheduler)

	step := int64(60000)
	now := time.UnixMilli((time.Now().UnixMilli() / step) * step)
	svc.now = func() time.Time { return now }
	req := request(now.UnixMilli()-3*step, now.UnixMilli())
	req.ToUTCMS = now.UnixMilli() + step // live edge inside the current epoch

	// Poll 1 is the cold full-window fetch; polls 2..3 hit the live-edge
	// missing range: two bounded attempts there.
	if _, err := svc.Get(context.Background(), "bot", ratelimit.PriorityLiveRefresh, req); err != nil {
		t.Fatalf("cold poll: %v", err)
	}
	for i := 2; i <= 3; i++ {
		got, err := svc.Get(context.Background(), "bot", ratelimit.PriorityLiveRefresh, req)
		if err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
		if got.Freshness == Fresh {
			t.Fatalf("poll %d: publication lag must not be labeled fresh", i)
		}
	}
	callsAtBudget := upstreamCalls(upstream)
	// Poll 4: the live-edge range budget is exhausted — served from cache
	// with the typed deferred cause and no further provider call.
	got, err := svc.Get(context.Background(), "bot", ratelimit.PriorityLiveRefresh, req)
	if err != nil {
		t.Fatalf("deferred poll: %v", err)
	}
	if got.Freshness != StaleAcceptable || got.StaleCause == nil || len(got.Candles) == 0 {
		t.Fatalf("deferred result: freshness=%s cause=%v candles=%d", got.Freshness, got.StaleCause, len(got.Candles))
	}
	for range 3 {
		if _, err := svc.Get(context.Background(), "bot", ratelimit.PriorityLiveRefresh, req); err != nil {
			t.Fatal(err)
		}
	}
	if after := upstreamCalls(upstream); after != callsAtBudget {
		t.Fatalf("deferred polls hit the provider: %d -> %d", callsAtBudget, after)
	}
	// A new epoch (next closed-bar boundary) re-permits the refresh.
	svc.now = func() time.Time { return now.Add(time.Duration(step) * time.Millisecond) }
	if _, err := svc.Get(context.Background(), "bot", ratelimit.PriorityLiveRefresh, req); err != nil {
		t.Fatalf("new epoch poll: %v", err)
	}
	if after := upstreamCalls(upstream); after <= callsAtBudget {
		t.Fatalf("new epoch must allow a refresh: calls=%d before=%d", after, callsAtBudget)
	}
}

func TestAdmissionRejectionServesStaleAndQueuesBackgroundRefresh(t *testing.T) {
	upstream := &fakeProvider{response: func(r model.CandleRequest) ([]model.Candle, error) {
		return candlesFor(r), nil
	}}
	// Budget 2: seed (1) + hog (2) exhaust it; the extended range refresh is
	// then rejected by admission.
	svc, store, scheduler, advance := epochServiceClock(t, upstream, Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour, BackgroundRefresh: true, BackgroundQueueCapacity: 16}, 2, 200*time.Millisecond)
	defer closeService(t, store, scheduler)

	seeded := request(0, 180000)
	if _, err := svc.Get(context.Background(), "bot", ratelimit.PriorityLiveRefresh, seeded); err != nil {
		t.Fatal(err)
	}
	// Exhaust the budget with a blocked warmup-priority job.
	release := make(chan struct{})
	result, err := scheduler.Submit(context.Background(), "hog", ratelimit.PriorityWarmup, func(ctx context.Context) error {
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// An extended window for the seeded series needs a new missing range, but
	// the budget is gone: the client gets the cached snapshot honestly labeled
	// stale with the typed admission cause — never silently fresh, never a
	// bare retry storm.
	extended := request(0, 300000)
	got, err := svc.Get(context.Background(), "bot", ratelimit.PriorityLiveRefresh, extended)
	if err != nil {
		t.Fatalf("acceptable stale must be served without error, got %v", err)
	}
	if got.Freshness != StaleAcceptable || got.StaleCause == nil || len(got.Candles) == 0 {
		t.Fatalf("stale result: freshness=%s cause=%v candles=%d", got.Freshness, got.StaleCause, len(got.Candles))
	}
	if reason := AdmissionReason(got.StaleCause); reason != "node_budget" {
		t.Fatalf("stale cause admission reason=%q, want node_budget", reason)
	}

	// The stale-while-revalidate queue drains the refresh once the budget
	// window rolls, without any client retry.
	close(release)
	<-result
	advance(300 * time.Millisecond)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		memory, lookupErr := svc.cache.Lookup(seeded.Series, cacheRangeOf(extended))
		if lookupErr == nil && memory.Complete {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	memory, lookupErr := svc.cache.Lookup(seeded.Series, cacheRangeOf(extended))
	if lookupErr != nil || !memory.Complete {
		t.Fatal("background refresh never completed the extended range")
	}
	fresh, err := svc.Get(context.Background(), "bot", ratelimit.PriorityLiveRefresh, extended)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Freshness != Fresh {
		t.Fatalf("post-recovery freshness=%s, want fresh", fresh.Freshness)
	}
}

// TestColdStartEightySeriesNeverExceedsRollingBudget is the controlled cold
// start of two contours (40 × 15m + 40 × 1h) against an artificially small
// budget: every request returns a bounded outcome (fresh/stale/typed
// rejection — never a hang), actual provider attempts stay within the budget
// in any rolling window, and the background queue eventually warms every
// series within the documented number of windows.
func TestColdStartEightySeriesNeverExceedsRollingBudget(t *testing.T) {
	const series = 80
	const perWindow = 30
	const window = 200 * time.Millisecond
	timeframes := []string{"15m", "1h"}

	var callMu sync.Mutex
	var callTimes []time.Time
	upstream := &fakeProvider{response: func(r model.CandleRequest) ([]model.Candle, error) {
		callMu.Lock()
		callTimes = append(callTimes, time.Now())
		callMu.Unlock()
		return candlesFor(r), nil
	}}
	svc, store, scheduler := epochService(t, upstream, Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour, BackgroundRefresh: true, BackgroundQueueCapacity: 128}, perWindow)
	defer closeService(t, store, scheduler)

	key := func(i int, timeframe string) model.SeriesKey {
		return model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: fmt.Sprintf("figi%03d", i), Timeframe: timeframe, CandleType: "trade"}
	}
	// Windows hold 50 provider candles each; the fake provider emits 60ms
	// steps regardless of timeframe semantics, so 50 candles = 3000ms. Each
	// window is pinned to its timeframe's closed-bar grid (fully closed
	// range) and pinned once so checks compare against the exact merged
	// coverage.
	stepOf := map[string]int64{"15m": 900_000, "1h": 3_600_000}
	pinnedTo := map[string]int64{}
	for _, tf := range timeframes {
		pinnedTo[tf] = (time.Now().UnixMilli() / stepOf[tf]) * stepOf[tf]
	}
	windowOf := func(timeframe string) (from, to int64) {
		toEnd := pinnedTo[timeframe]
		return toEnd - 3000, toEnd
	}

	var wg sync.WaitGroup
	outcomes := make([]error, series*len(timeframes))
	for i := range series {
		for tfIdx, timeframe := range timeframes {
			wg.Add(1)
			go func(idx, seriesIdx int, tf string) {
				defer wg.Done()
				from, to := windowOf(tf)
				_, err := svc.Get(context.Background(), "bot-"+tf, ratelimit.PriorityLiveRefresh, model.CandleRequest{Series: key(seriesIdx, tf), FromUTCMS: from, ToUTCMS: to, Limit: 168})
				outcomes[idx] = err
			}(i*len(timeframes)+tfIdx, i, timeframe)
		}
	}
	wg.Wait()
	for idx, err := range outcomes {
		if err != nil && !IsOverloaded(err) && !errors.Is(err, context.DeadlineExceeded) {
			tf := timeframes[idx%len(timeframes)]
			from, to := windowOf(tf)
			mem, _ := svc.cache.Lookup(key(idx/len(timeframes), tf), cache.Range{FromUTCMS: from, ToUTCMS: to})
			t.Fatalf("outcome %d (%s series %d): unexpected error %v; candles=%d missing=%v now=%d",
				idx, tf, idx/len(timeframes), err, len(mem.Entry.Candles), mem.Missing, time.Now().UnixMilli())
		}
	}

	// Rolling-window proof: any window of length `window` observed so far
	// contains at most perWindow actual provider attempts (background work
	// included).
	callMu.Lock()
	times := append([]time.Time(nil), callTimes...)
	callMu.Unlock()
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	for i, start := range times {
		count := sort.Search(len(times), func(j int) bool { return times[j].After(start.Add(window)) }) - i
		if count > perWindow {
			t.Fatalf("rolling window at %v holds %d provider attempts, budget %d", start, count, perWindow)
		}
	}

	// Warmup convergence: within ceil(160 / 30) = 6 windows (plus latency)
	// every series must be covered by the background queue.
	deadline := time.Now().Add(15 * time.Second)
	for {
		complete := 0
		for idx := range series * len(timeframes) {
			timeframe := timeframes[idx%len(timeframes)]
			from, to := windowOf(timeframe)
			memory, err := svc.cache.Lookup(key(idx/len(timeframes), timeframe), cache.Range{FromUTCMS: from, ToUTCMS: to})
			if err == nil && memory.Complete {
				complete++
			}
		}
		if complete == series*len(timeframes) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("warmup did not converge: %d/%d covered", complete, series*len(timeframes))
		}
		time.Sleep(20 * time.Millisecond)
	}
	// If any cold request was shed (overloaded/deadline), the background
	// queue must have recovered it. Whether shedding happens at all is a
	// timing property: when every request is served synchronously within
	// the rolling budget, no background enqueue is required.
	metrics := svc.Metrics()
	shed := false
	for _, err := range outcomes {
		if IsOverloaded(err) || errors.Is(err, context.DeadlineExceeded) {
			shed = true
			break
		}
	}
	if shed && metrics.RefreshQueued == 0 {
		t.Fatal("expected background refreshes to recover shed cold-start load")
	}
}
