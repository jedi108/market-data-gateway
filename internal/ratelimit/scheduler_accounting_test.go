// Task 109 accounting proofs: the node budget bounds ACTUAL provider
// attempts (including internal retries) in any window, and work that never
// reaches a provider call (queued-cancelled, evicted, drained) refunds its
// reservation instead of burning it.
package ratelimit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func accountingConfig(budget, retries int, window time.Duration) Config {
	return Config{
		NodeBudgetPerMinute:     budget,
		PerClientQuotaPerMinute: budget,
		MaxTrackedClients:       16,
		QueueCapacity:           256,
		WorkerCount:             2,
		MaxRetries:              retries,
		RetryBaseDelay:          time.Millisecond,
		Window:                  window,
		IsRetryable:             func(err error) bool { return errors.Is(err, errRetryableProbe) },
		Jitter:                  func(d time.Duration) time.Duration { return d },
	}
}

var errRetryableProbe = errors.New("retryable probe failure")

// TestSchedulerChargesRetriesAsProviderAttempts proves with max_retries > 0
// that the budget bounds actual provider calls, not accepted jobs: jobs that
// want more attempts than the budget allows are truncated at the budget.
func TestSchedulerChargesRetriesAsProviderAttempts(t *testing.T) {
	const budget = 3
	s := NewScheduler(accountingConfig(budget, 5, time.Second))
	var calls atomic.Int32
	run := func() error {
		result, err := s.Submit(context.Background(), "client", PriorityLiveRefresh, func(ctx context.Context) error {
			calls.Add(1)
			return errRetryableProbe
		})
		if err != nil {
			return err
		}
		return <-result
	}
	for range budget {
		if err := run(); err == nil {
			t.Fatal("expected probe failure result")
		}
	}
	if got := calls.Load(); got != budget {
		t.Fatalf("provider calls=%d, want exactly the budget %d (retries must charge real attempts)", got, budget)
	}
	if s.Snapshot().RetriesSkippedBudget == 0 {
		t.Fatal("expected retries to be skipped once the budget was exhausted")
	}
	// A further submission is rejected on the reserved budget, not silently
	// over-consumed.
	if _, err := s.Submit(context.Background(), "client", PriorityLiveRefresh, func(ctx context.Context) error { return nil }); !errors.Is(err, ErrNodeBudget) {
		t.Fatalf("submit after budget exhaustion: err=%v, want ErrNodeBudget", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestSchedulerRefundsQueuedCancelledAndEvictedJobs proves queued work that
// never attempts the provider does not consume budget: the evicted warmup
// job's reservation is refunded, so the window's used count reflects only
// actual provider attempts.
func TestSchedulerRefundsQueuedCancelledAndEvictedJobs(t *testing.T) {
	cfg := accountingConfig(8, 0, time.Second)
	cfg.QueueCapacity = 2
	cfg.WorkerCount = 1
	s := NewScheduler(cfg)

	// One blocked in-flight job occupies the single worker. The pickup signal
	// fires only after the worker has popped it, so the queue is empty again.
	release := make(chan struct{})
	picked := make(chan struct{})
	blocked, err := s.Submit(context.Background(), "client", PriorityLiveRefresh, func(ctx context.Context) error {
		picked <- struct{}{}
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	<-picked

	// Fill the bounded queue with warmup jobs.
	warmups := make([]<-chan error, 0, 2)
	for range 2 {
		result, err := s.Submit(context.Background(), "warm", PriorityWarmup, func(ctx context.Context) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		warmups = append(warmups, result)
	}

	// A live submission into the full queue evicts the queued warmup; the
	// eviction refunds the warmup reservation, so the live job is admitted
	// within the same window.
	live, err := s.Submit(context.Background(), "client", PriorityLiveRefresh, func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatalf("live submit after warmup eviction: %v (eviction must refund)", err)
	}
	if err := <-warmups[0]; !errors.Is(err, ErrQueueFull) {
		t.Fatalf("evicted warmup result=%v, want ErrQueueFull", err)
	}
	close(release)
	if err := <-live; err != nil {
		t.Fatal(err)
	}
	if err := <-blocked; err != nil {
		t.Fatal(err)
	}
	if err := <-warmups[1]; err != nil {
		t.Fatal(err)
	}
	used := s.Snapshot().NodeBudgetUsed
	// blocked(1) + live(1) + surviving warmup(1) = 3; the evicted job's
	// reservation was refunded — without the refund this would be 4.
	if used != 3 {
		t.Fatalf("budget used=%d, want 3 (eviction must refund its reservation)", used)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestSchedulerDrainRefundsQueuedReservations proves the drain deadline path
// refunds queued reservations: they never become provider calls.
func TestSchedulerDrainRefundsQueuedReservations(t *testing.T) {
	cfg := accountingConfig(4, 0, time.Second)
	cfg.WorkerCount = 1
	s := NewScheduler(cfg)

	// The blocked job keeps holding the single worker while the drain
	// deadline passes, so the queued jobs must be cancelled and refunded.
	hold := make(chan struct{})
	blocked, err := s.Submit(context.Background(), "client", PriorityLiveRefresh, func(ctx context.Context) error {
		select {
		case <-hold:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	queued := make([]<-chan error, 0, 3)
	for range 3 {
		result, err := s.Submit(context.Background(), "client", PriorityLiveRefresh, func(ctx context.Context) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		queued = append(queued, result)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = s.Drain(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain err=%v, want deadline (jobs still queued)", err)
	}
	for _, result := range queued {
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("queued result=%v, want cancellation refund", err)
		}
	}
	close(hold)
	if err := <-blocked; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked result=%v, want cancellation at drain", err)
	}
}

// TestSchedulerNextAllowedAtDerivesRetryAfter proves the computed Retry-After
// contract: with free capacity the hint is zero; with an exhausted window it
// is the accounting window's rollover, not a hardcoded constant.
func TestSchedulerNextAllowedAtDerivesRetryAfter(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	cfg := accountingConfig(1, 0, 30*time.Second)
	cfg.Now = func() time.Time { return now }
	s := NewScheduler(cfg)

	if !s.NextAllowedAt().IsZero() {
		t.Fatal("empty window must report free capacity")
	}
	result, err := s.Submit(context.Background(), "client", PriorityLiveRefresh, func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-result
	next := s.NextAllowedAt()
	if want := now.Add(30 * time.Second); !next.Equal(want) {
		t.Fatalf("next allowed=%v, want window rollover %v", next, want)
	}
}

// TestSchedulerBudgetBoundsConcurrentBurst: 80 concurrent submissions against
// an artificially small budget produce at most `budget` actual provider
// calls; every rejected caller observes a typed admission error.
func TestSchedulerBudgetBoundsConcurrentBurst(t *testing.T) {
	const budget = 8
	const burst = 80
	s := NewScheduler(accountingConfig(budget, 0, time.Second))
	var calls atomic.Int32
	var admitted atomic.Int32
	var rejected atomic.Int32
	release := make(chan struct{})
	results := make([]<-chan error, 0, burst)
	for range burst {
		result, err := s.Submit(context.Background(), "burst", PriorityLiveRefresh, func(ctx context.Context) error {
			calls.Add(1)
			<-release
			return nil
		})
		if err != nil {
			rejected.Add(1)
			continue
		}
		admitted.Add(1)
		results = append(results, result)
	}
	close(release)
	for _, result := range results {
		if err := <-result; err != nil {
			t.Fatalf("admitted job failed: %v", err)
		}
	}
	if got := calls.Load(); got > budget {
		t.Fatalf("provider calls=%d exceed budget %d", got, budget)
	}
	if rejected.Load() == 0 {
		t.Fatal("expected typed admission rejections under burst")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}
