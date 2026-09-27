package ratelimit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		NodeBudgetPerMinute:     100,
		PerClientQuotaPerMinute: 10,
		MaxTrackedClients:       4,
		QueueCapacity:           2,
		WorkerCount:             1,
		MaxRetries:              1,
		RetryBaseDelay:          time.Millisecond,
	}
}

func TestSchedulerRejectsWarmupWhenBoundedQueueIsFull(t *testing.T) {
	cfg := testConfig()
	cfg.QueueCapacity = 1
	started := make(chan struct{})
	release := make(chan struct{})
	s := NewScheduler(cfg)
	defer shutdownTestScheduler(t, s)

	first, err := s.Submit(context.Background(), "client-a", PriorityLiveRefresh, func(context.Context) error {
		close(started)
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("Submit first job: %v", err)
	}
	<-started
	second, err := s.Submit(context.Background(), "client-b", PriorityLiveRefresh, func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("Submit queued job: %v", err)
	}
	if _, err := s.Submit(context.Background(), "client-c", PriorityWarmup, func(context.Context) error { return nil }); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Submit warmup while full error = %v, want ErrQueueFull", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first result = %v", err)
	}
	if err := <-second; err != nil {
		t.Fatalf("second result = %v", err)
	}
}

func TestSchedulerPrefersLiveRefreshOverQueuedWarmup(t *testing.T) {
	cfg := testConfig()
	cfg.QueueCapacity = 3
	started := make(chan struct{})
	release := make(chan struct{})
	order := make(chan string, 2)
	s := NewScheduler(cfg)
	defer shutdownTestScheduler(t, s)

	first, err := s.Submit(context.Background(), "client-a", PriorityLiveRefresh, func(context.Context) error {
		close(started)
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	warmup, err := s.Submit(context.Background(), "client-b", PriorityWarmup, func(context.Context) error { order <- "warmup"; return nil })
	if err != nil {
		t.Fatal(err)
	}
	live, err := s.Submit(context.Background(), "client-c", PriorityLiveRefresh, func(context.Context) error { order <- "live"; return nil })
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if got := <-order; got != "live" {
		t.Fatalf("first queued priority = %q, want live", got)
	}
	if err := <-live; err != nil {
		t.Fatal(err)
	}
	if err := <-warmup; err != nil {
		t.Fatal(err)
	}
}

func TestSchedulerEnforcesPerClientQuota(t *testing.T) {
	cfg := testConfig()
	cfg.PerClientQuotaPerMinute = 1
	s := NewScheduler(cfg)
	defer shutdownTestScheduler(t, s)

	first, err := s.Submit(context.Background(), "client-a", PriorityLiveRefresh, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(context.Background(), "client-a", PriorityLiveRefresh, func(context.Context) error { return nil }); !errors.Is(err, ErrClientQuota) {
		t.Fatalf("second client request error = %v, want ErrClientQuota", err)
	}
}

func TestSchedulerExpiresInactiveClientTrackingSlots(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cfg := testConfig()
	cfg.MaxTrackedClients = 1
	cfg.Now = func() time.Time { return now }
	s := NewScheduler(cfg)
	defer shutdownTestScheduler(t, s)

	first, err := s.Submit(context.Background(), "client-a", PriorityLiveRefresh, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	second, err := s.Submit(context.Background(), "client-b", PriorityLiveRefresh, func(context.Context) error { return nil })
	if err != nil {
		t.Fatalf("Submit after client window expiry: %v", err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}

func TestSchedulerDoesNotRetryCanceledRequest(t *testing.T) {
	s := NewScheduler(testConfig())
	defer shutdownTestScheduler(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls atomic.Int32
	if _, err := s.Submit(ctx, "client-a", PriorityLiveRefresh, func(context.Context) error {
		calls.Add(1)
		return errors.New("temporary")
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit canceled context error = %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("operation calls = %d, want 0", got)
	}
}

func TestSchedulerRetriesRetryableErrorWithinDeadline(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRetries = 2
	cfg.IsRetryable = func(error) bool { return true }
	cfg.Jitter = func(d time.Duration) time.Duration { return 0 }
	s := NewScheduler(cfg)
	defer shutdownTestScheduler(t, s)
	var calls atomic.Int32
	result, err := s.Submit(context.Background(), "client-a", PriorityLiveRefresh, func(context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("temporary")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("result = %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("operation calls = %d, want 3", got)
	}
}

func TestSchedulerDrainRejectsNewWorkAndCancelsRemainderAtDeadline(t *testing.T) {
	s := NewScheduler(testConfig())
	started := make(chan struct{})
	result, err := s.Submit(context.Background(), "client-a", PriorityLiveRefresh, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Drain(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain error = %v, want deadline exceeded", err)
	}
	if _, err := s.Submit(context.Background(), "client-b", PriorityLiveRefresh, func(context.Context) error { return nil }); !errors.Is(err, ErrDraining) {
		t.Fatalf("Submit after drain error = %v, want ErrDraining", err)
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("in-flight result = %v, want canceled", err)
	}
}

func shutdownTestScheduler(t *testing.T, s *Scheduler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
}

func TestSchedulerSnapshotReportsQueueBudgetAndClients(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	s := NewScheduler(testConfig())
	result, err := s.Submit(context.Background(), "client-a", PriorityLiveRefresh, func(context.Context) error {
		close(started)
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started // the live job is now executing (in-flight, not queued)
	if _, err := s.Submit(context.Background(), "client-b", PriorityWarmup, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	snapshot := s.Snapshot()
	if snapshot.InFlight != 1 || snapshot.QueueDepthWarmup != 1 || snapshot.NodeBudgetUsed != 2 || snapshot.TrackedClients != 2 || snapshot.NodeBudgetLimit != testConfig().NodeBudgetPerMinute {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	shutdownTestScheduler(t, s)
}
