// Package ratelimit provides bounded admission, scheduling, retry, and draining
// for upstream market-data calls. It intentionally owns a fixed worker set and
// bounded work/result channels only.
package ratelimit

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

var (
	ErrQueueFull              = errors.New("gateway queue is full")
	ErrClientQuota            = errors.New("client quota exhausted")
	ErrNodeBudget             = errors.New("node upstream budget exhausted")
	ErrClientTrackingCapacity = errors.New("client tracking capacity exhausted")
	ErrDraining               = errors.New("gateway is draining")
)

type Priority uint8

const (
	PriorityWarmup Priority = iota
	PriorityLiveRefresh
)

type Config struct {
	NodeBudgetPerMinute     int
	PerClientQuotaPerMinute int
	MaxTrackedClients       int
	QueueCapacity           int
	WorkerCount             int
	MaxRetries              int
	RetryBaseDelay          time.Duration
	// Window is the sliding budget-window length. It defaults to one minute;
	// tests shrink it to keep rolling-window proofs fast.
	Window      time.Duration
	IsRetryable func(error) bool
	Jitter      func(time.Duration) time.Duration
	Now         func() time.Time
}

// usage is a sliding-window attempt log (task 109): the budget bounds
// attempts in ANY window of the configured length, not a fixed/tumbling
// bucket. Entries outside the window are pruned lazily.
type usage struct {
	times []time.Time
}

func (u usage) prune(now time.Time, window time.Duration) usage {
	cut := 0
	for cut < len(u.times) && now.Sub(u.times[cut]) >= window {
		cut++
	}
	if cut == 0 {
		return u
	}
	u.times = append([]time.Time(nil), u.times[cut:]...)
	return u
}

func (u usage) countUsage() int { return len(u.times) }

func (u usage) remove(at time.Time) usage {
	for i, t := range u.times {
		if t.Equal(at) {
			u.times = append(u.times[:i], u.times[i+1:]...)
			return u
		}
	}
	return u
}

type job struct {
	ctx      context.Context
	clientID string
	priority Priority
	op       func(context.Context) error
	result   chan error
	// reservedAt is the sliding-window timestamp this job's reservation
	// occupies; the exact entry is removed on refund.
	reservedAt time.Time
}

type Scheduler struct {
	cfg Config

	mu          sync.Mutex
	cond        *sync.Cond
	queue       []job
	clients     map[string]usage
	node        usage
	outstanding int
	draining    bool
	stopped     bool
	idle        chan struct{}

	// Task 109 admission accounting: every submitted job is counted, and every
	// rejection carries its typed reason so backpressure is observable without
	// high-cardinality labels.
	admitted             uint64
	rejectedNodeBudget   uint64
	rejectedClientQuota  uint64
	rejectedQueueFull    uint64
	rejectedTracking     uint64
	rejectedDraining     uint64
	retriesSkippedBudget uint64

	runCtx  context.Context
	cancel  context.CancelFunc
	workers sync.WaitGroup
}

func NewScheduler(cfg Config) *Scheduler {
	if cfg.NodeBudgetPerMinute <= 0 || cfg.PerClientQuotaPerMinute <= 0 || cfg.MaxTrackedClients <= 0 || cfg.QueueCapacity <= 0 || cfg.WorkerCount <= 0 || cfg.MaxRetries < 0 || cfg.RetryBaseDelay <= 0 {
		panic("ratelimit: invalid required scheduler configuration")
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.IsRetryable == nil {
		cfg.IsRetryable = func(error) bool { return false }
	}
	if cfg.Jitter == nil {
		cfg.Jitter = defaultJitter
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	runCtx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		cfg:     cfg,
		clients: make(map[string]usage, cfg.MaxTrackedClients),
		idle:    make(chan struct{}, 1),
		runCtx:  runCtx,
		cancel:  cancel,
	}
	s.cond = sync.NewCond(&s.mu)
	s.workers.Add(cfg.WorkerCount)
	for range cfg.WorkerCount {
		go s.worker()
	}
	return s
}

// Submit admits one upstream call. A successful submission always owns exactly
// one bounded result channel. A caller's context deadline is carried to the
// worker and every retry; it never creates a per-request goroutine.
//
// Task 109 accounting contract: submission reserves exactly one provider
// attempt. That reservation is refundable atomically before any provider call
// — a queued job that is cancelled, evicted, or drained never burns its
// token. Internal retries are charged immediately before each retry attempt;
// a retry that cannot be charged is skipped, so actual provider calls never
// exceed the budget in any window.
func (s *Scheduler) Submit(ctx context.Context, clientID string, priority Priority, op func(context.Context) error) (<-chan error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if clientID == "" || op == nil || (priority != PriorityLiveRefresh && priority != PriorityWarmup) {
		return nil, errors.New("invalid scheduler job")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining || s.stopped {
		s.rejectedDraining++
		return nil, ErrDraining
	}
	if len(s.queue) >= s.cfg.QueueCapacity && priority == PriorityLiveRefresh {
		if !s.evictWarmupLocked() {
			s.rejectedQueueFull++
			return nil, ErrQueueFull
		}
	}
	if len(s.queue) >= s.cfg.QueueCapacity {
		s.rejectedQueueFull++
		return nil, ErrQueueFull
	}
	reservedAt, err := s.reserveLocked(clientID, s.cfg.Now())
	if err != nil {
		switch {
		case errors.Is(err, ErrNodeBudget):
			s.rejectedNodeBudget++
		case errors.Is(err, ErrClientQuota):
			s.rejectedClientQuota++
		default:
			s.rejectedTracking++
		}
		return nil, err
	}
	s.admitted++

	result := make(chan error, 1)
	s.queue = append(s.queue, job{ctx: ctx, clientID: clientID, priority: priority, op: op, result: result, reservedAt: reservedAt})
	s.outstanding++
	s.cond.Signal()
	return result, nil
}

func (s *Scheduler) reserveLocked(clientID string, now time.Time) (time.Time, error) {
	var zero time.Time
	s.node = s.node.prune(now, s.cfg.Window)
	if len(s.node.times) >= s.cfg.NodeBudgetPerMinute {
		return zero, ErrNodeBudget
	}
	s.pruneClientsLocked(now)
	client, found := s.clients[clientID]
	if !found && len(s.clients) >= s.cfg.MaxTrackedClients {
		return zero, ErrClientTrackingCapacity
	}
	client = client.prune(now, s.cfg.Window)
	if len(client.times) >= s.cfg.PerClientQuotaPerMinute {
		s.clients[clientID] = client
		return zero, ErrClientQuota
	}
	client.times = append(client.times, now)
	s.clients[clientID] = client
	s.node.times = append(s.node.times, now)
	return now, nil
}

// pruneClientsLocked drops idle clients whose sliding window is empty so the
// tracking capacity reflects active callers only.
func (s *Scheduler) pruneClientsLocked(now time.Time) {
	for clientID, client := range s.clients {
		client = client.prune(now, s.cfg.Window)
		if len(client.times) == 0 {
			delete(s.clients, clientID)
			continue
		}
		s.clients[clientID] = client
	}
}

// refundLocked returns one reserved attempt that will never become a provider
// call. The exact reserved timestamp is removed — the atomic return path
// required by the task 109 invariant. A reservation older than the window has
// already expired on its own.
func (s *Scheduler) refundLocked(clientID string, reservedAt time.Time) {
	if reservedAt.IsZero() {
		return
	}
	now := s.cfg.Now()
	s.node = s.node.prune(now, s.cfg.Window)
	s.node = s.node.remove(reservedAt)
	if client, found := s.clients[clientID]; found {
		client = client.prune(now, s.cfg.Window).remove(reservedAt)
		if len(client.times) == 0 {
			delete(s.clients, clientID)
		} else {
			s.clients[clientID] = client
		}
	}
}

// chargeRetryLocked reserves one additional provider attempt for an internal
// retry, immediately before that attempt. Budget exhaustion here skips the
// retry instead of exceeding the provider budget. The returned timestamp is
// ignored (retries are never refunded separately).
func (s *Scheduler) chargeRetryLocked(clientID string) error {
	_, err := s.reserveLocked(clientID, s.cfg.Now())
	return err
}

func (s *Scheduler) evictWarmupLocked() bool {
	for i, queued := range s.queue {
		if queued.priority != PriorityWarmup {
			continue
		}
		s.queue = append(s.queue[:i], s.queue[i+1:]...)
		s.outstanding--
		// The evicted job never reaches a provider call, so its reserved
		// attempt is returned atomically (task 109).
		s.refundLocked(queued.clientID, queued.reservedAt)
		queued.result <- ErrQueueFull
		s.signalIdleLocked()
		return true
	}
	return false
}

func (s *Scheduler) worker() {
	defer s.workers.Done()
	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.stopped {
			s.cond.Wait()
		}
		if s.stopped {
			s.mu.Unlock()
			return
		}
		current := s.popLocked()
		if current.ctx.Err() != nil {
			// Cancelled while queued: the reserved attempt is refunded and
			// the caller observes the cancellation. No provider call happens.
			s.refundLocked(current.clientID, current.reservedAt)
			s.outstanding--
			s.signalIdleLocked()
			s.mu.Unlock()
			current.result <- current.ctx.Err()
			continue
		}
		s.mu.Unlock()

		err, attempted := s.execute(current)
		if !attempted {
			s.mu.Lock()
			s.refundLocked(current.clientID, current.reservedAt)
			s.mu.Unlock()
		}
		current.result <- err
		s.mu.Lock()
		s.outstanding--
		s.signalIdleLocked()
		s.mu.Unlock()
	}
}

func (s *Scheduler) popLocked() job {
	for i, queued := range s.queue {
		if queued.priority == PriorityLiveRefresh {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return queued
		}
	}
	current := s.queue[0]
	s.queue = s.queue[1:]
	return current
}

// execute runs one admitted job. The first provider attempt consumes the
// reservation made at Submit time; every internal retry is charged immediately
// before its attempt and is skipped when the budget cannot cover it, so the
// number of actual provider calls never exceeds the node budget in any
// window. attempted reports whether at least one attempt was charged; a job
// that never attempted (cancelled before the first call) gets its reservation
// refunded by the worker.
func (s *Scheduler) execute(current job) (err error, attempted bool) {
	ctx, cancel := context.WithCancel(current.ctx)
	defer cancel()
	stop := context.AfterFunc(s.runCtx, cancel)
	defer stop()

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err, attempted
		}
		if attempt > 0 {
			s.mu.Lock()
			chargeErr := s.chargeRetryLocked(current.clientID)
			if chargeErr != nil {
				s.retriesSkippedBudget++
				s.mu.Unlock()
				// Budget cannot cover this retry: the previous upstream error
				// stands (it is the honest provider outcome), and no extra
				// provider call is made.
				return err, attempted
			}
			s.mu.Unlock()
		}
		err = current.op(ctx)
		attempted = true
		if err == nil || !s.cfg.IsRetryable(err) || attempt >= s.cfg.MaxRetries {
			return err, attempted
		}
		delay := s.cfg.Jitter(backoff(s.cfg.RetryBaseDelay, attempt))
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err(), attempted
		case <-timer.C:
		}
	}
}

func backoff(base time.Duration, attempt int) time.Duration {
	for range attempt {
		if base > time.Duration(1<<62) {
			return time.Duration(1 << 62)
		}
		base *= 2
	}
	return base
}

func defaultJitter(delay time.Duration) time.Duration {
	if delay <= 1 {
		return delay
	}
	return delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
}

// Snapshot exposes bounded scheduler gauges for observability. It contains
// operational counters only: no client identity is reflected in labels.
type Snapshot struct {
	QueueDepthLiveRefresh int
	QueueDepthWarmup      int
	InFlight              int
	NodeBudgetLimit       int
	NodeBudgetUsed        int
	TrackedClients        int
	// Task 109 admission observability: admitted/rejected jobs by typed
	// reason, plus retries skipped for budget. All bounded vocabularies.
	Admitted             uint64
	RejectedNodeBudget   uint64
	RejectedClientQuota  uint64
	RejectedQueueFull    uint64
	RejectedTracking     uint64
	RejectedDraining     uint64
	RetriesSkippedBudget uint64
}

func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := Snapshot{
		InFlight:             s.outstanding - len(s.queue),
		NodeBudgetLimit:      s.cfg.NodeBudgetPerMinute,
		NodeBudgetUsed:       s.node.prune(s.cfg.Now(), s.cfg.Window).countUsage(),
		TrackedClients:       len(s.clients),
		Admitted:             s.admitted,
		RejectedNodeBudget:   s.rejectedNodeBudget,
		RejectedClientQuota:  s.rejectedClientQuota,
		RejectedQueueFull:    s.rejectedQueueFull,
		RejectedTracking:     s.rejectedTracking,
		RejectedDraining:     s.rejectedDraining,
		RetriesSkippedBudget: s.retriesSkippedBudget,
	}
	for _, queued := range s.queue {
		if queued.priority == PriorityLiveRefresh {
			snapshot.QueueDepthLiveRefresh++
		} else {
			snapshot.QueueDepthWarmup++
		}
	}
	return snapshot
}

// NextAllowedAt returns the earliest time the node budget can admit another
// provider attempt, or the zero time when capacity is available now. It powers
// the computed Retry-After contract (task 109): backpressure hints are derived
// from the actual accounting window, never hardcoded.
func (s *Scheduler) NextAllowedAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.node = s.node.prune(s.cfg.Now(), s.cfg.Window)
	if len(s.node.times) < s.cfg.NodeBudgetPerMinute {
		return time.Time{}
	}
	// The oldest logged attempt frees its slot exactly one window later.
	return s.node.times[0].Add(s.cfg.Window)
}

// Drain stops admission immediately, completes queued and in-flight work while
// ctx remains valid, then cancels and reports the remaining work at its deadline.
func (s *Scheduler) Drain(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.draining = true
	s.cond.Broadcast()
	s.mu.Unlock()

	for {
		s.mu.Lock()
		if s.outstanding == 0 {
			s.stopLocked()
			s.mu.Unlock()
			s.workers.Wait()
			return nil
		}
		s.mu.Unlock()

		select {
		case <-s.idle:
		case <-ctx.Done():
			s.mu.Lock()
			s.cancel()
			for _, queued := range s.queue {
				// Queued jobs cancelled by the drain deadline never reach a
				// provider call; their reserved attempts are refunded.
				s.refundLocked(queued.clientID, queued.reservedAt)
				queued.result <- context.Canceled
			}
			s.outstanding -= len(s.queue)
			s.queue = nil
			s.stopLocked()
			s.mu.Unlock()
			s.workers.Wait()
			return ctx.Err()
		}
	}
}

func (s *Scheduler) stopLocked() {
	if s.stopped {
		return
	}
	s.stopped = true
	s.cancel()
	s.cond.Broadcast()
}

func (s *Scheduler) signalIdleLocked() {
	select {
	case s.idle <- struct{}{}:
	default:
	}
}
