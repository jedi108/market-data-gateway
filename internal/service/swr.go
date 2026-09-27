// Task 109 stale-while-revalidate queue: when admission pressure forces a
// stale/fail-closed response, exactly one deduplicated bounded background
// refresh per series is queued here so recovery does not depend on client
// retries. The queue never creates unbounded work: capacity is fixed,
// duplicate series are coalesced, jobs are drained through the normal
// scheduler admission path, and stop() refuses everything after shutdown.
package service

import (
	"context"
	"sync"
	"time"

	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
)

// defaultBackgroundQueueCapacity bounds the stale-while-revalidate queue when
// the configuration does not select a value.
const defaultBackgroundQueueCapacity = 64

// backgroundPriority is the scheduler priority of background recovery work:
// it always yields to live-refresh traffic and is evicted first.
const backgroundPriority = ratelimit.PriorityWarmup

// Bounded retry policy for background jobs rejected by admission: a job that
// could not be scheduled (budget exhausted by live traffic) re-queues a
// limited number of times before giving up. The next client poll re-arms the
// path regardless, so this only smooths the race into the same exhausted
// window.
const (
	maxSWRAttempts = 8
	swrRetryDelay  = 250 * time.Millisecond
)

type swrJob struct {
	clientID  string
	request   model.CandleRequest
	requested cache.Range
	attempts  int
}

// refreshQueue is a bounded, deduplicating FIFO of background refresh jobs
// served by a single worker (the scheduler provides the real concurrency
// bound; the queue only bounds pending work).
type refreshQueue struct {
	service *Service
	max     int

	mu      sync.Mutex
	cond    *sync.Cond
	queue   []swrJob
	pending map[string]struct{}
	stopped bool
	done    chan struct{}
}

func newRefreshQueue(s *Service, capacity int) *refreshQueue {
	if capacity <= 0 {
		capacity = defaultBackgroundQueueCapacity
	}
	q := &refreshQueue{service: s, max: capacity, pending: make(map[string]struct{}), done: make(chan struct{})}
	q.cond = sync.NewCond(&q.mu)
	go q.worker()
	return q
}

// enqueue adds one job unless the series is already queued or the queue is
// full/stopped. It reports whether the job was accepted.
func (q *refreshQueue) enqueue(job swrJob) bool {
	identity := job.request.Series.Identity()
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return false
	}
	if _, dup := q.pending[identity]; dup {
		return true // exactly one bounded refresh per series at a time
	}
	if len(q.queue) >= q.max {
		return false
	}
	q.pending[identity] = struct{}{}
	q.queue = append(q.queue, job)
	q.cond.Signal()
	return true
}

// pop blocks for the next job; ok=false means the queue was stopped.
func (q *refreshQueue) pop() (swrJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.queue) == 0 && !q.stopped {
		q.cond.Wait()
	}
	if len(q.queue) == 0 {
		return swrJob{}, false
	}
	job := q.queue[0]
	q.queue = q.queue[1:]
	delete(q.pending, job.request.Series.Identity())
	return job, true
}

func (q *refreshQueue) stop() {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.stopped = true
	// Queued-but-unstarted jobs are dropped: their series were already
	// answered with typed stale/fail-closed responses, and shutdown must not
	// start new provider work. In-flight jobs are bounded by the request
	// timeout and scheduler admission.
	q.queue = nil
	q.pending = make(map[string]struct{})
	q.cond.Broadcast()
	q.mu.Unlock()
	<-q.done
}

func (q *refreshQueue) worker() {
	defer close(q.done)
	for {
		job, ok := q.pop()
		if !ok {
			return
		}
		err := q.service.runBackgroundRefresh(job)
		// Bounded re-queue on admission rejection: the budget was consumed
		// by live traffic; retry a few times before giving up.
		if err != nil && IsAdmissionRejection(err) && job.attempts+1 < maxSWRAttempts {
			job.attempts++
			time.AfterFunc(swrRetryDelay, func() { q.reenqueue(job) })
		}
	}
}

// reenqueue returns a failed job to the queue after a delay, respecting the
// same dedup and bounds as enqueue.
func (q *refreshQueue) reenqueue(job swrJob) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return
	}
	if _, dup := q.pending[job.request.Series.Identity()]; dup {
		return
	}
	if len(q.queue) >= q.max {
		return
	}
	q.pending[job.request.Series.Identity()] = struct{}{}
	q.queue = append(q.queue, job)
	q.cond.Signal()
}

// runBackgroundRefresh executes one queued refresh through the normal
// singleflight + scheduler admission path at warmup priority. The returned
// error drives the worker's bounded retry policy; failures never re-enqueue
// on their own beyond that.
func (s *Service) runBackgroundRefresh(job swrJob) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.RequestTimeout)
	defer cancel()
	_, err := s.cache.Do(ctx, job.request.Series, func() error {
		return s.refresh(job.clientID, backgroundPriority, job.request, job.requested)
	})
	// Record the outcome so deferred/fail-closed responses carry the honest
	// typed cause instead of an empty-context failure.
	s.noteEpochOutcome(job.request.Series, err)
	if err != nil {
		s.logWarn("background refresh failed", job.request.Series, "reason", upstreamStatus(err))
		return err
	}
	s.logInfo("background refresh completed", job.request.Series)
	return nil
}
