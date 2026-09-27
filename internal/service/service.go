// Package service composes cache, durable storage, bounded admission, and the
// provider boundary into the local gateway candle request path.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/budget"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/cluster"
	"github.com/jedi108/market-data-gateway/internal/lease"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/observability"
	"github.com/jedi108/market-data-gateway/internal/provider"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

type CacheStatus string

const (
	CacheMemoryHit     CacheStatus = "memory_hit"
	CachePersistentHit CacheStatus = "persistent_hit"
	CacheMiss          CacheStatus = "miss"
	CacheRefreshed     CacheStatus = "refreshed"
	CacheSingleflight  CacheStatus = "singleflight_join"
)

type Freshness string

const (
	Fresh           Freshness = "fresh"
	StaleAcceptable Freshness = "stale_acceptable"
	StaleRejected   Freshness = "stale_rejected"
)

var ErrIncompleteCoverage = apperror.New(apperror.CodeIncompleteCoverage, "provider response did not cover the requested candle interval", 0)

// Result is the per-request outcome. Freshness and StaleCause carry the
// honest degradation state to the client: stale data is never labeled fresh,
// and the typed cause explains why a series is being served from cache
// (task 109 invariants).
type Result struct {
	Candles              []model.Candle
	CacheStatus          CacheStatus
	Freshness            Freshness
	Replica              bool
	SourceFetchedAtUTCMS int64
	ProviderStatus       string
	StaleCause           error
}

type Metrics struct {
	MemoryHits, MemoryMisses, SingleflightJoins      uint64
	PersistentHits, ReplicaResponses, StaleResponses uint64
	// Task 109 cache-first observability: bounded stale-while-revalidate and
	// warmup counters (no client identity in labels).
	RefreshQueued, RefreshDropped, RefreshDeferred uint64
	WarmupCompleted, WarmupSkipped                 uint64
}

type Config struct {
	RequestTimeout time.Duration
	MaxStaleAge    time.Duration
	// Router decides series ownership. When nil the node treats every series
	// as locally owned (Phase 1 / single-node mode).
	Router Router
	// Peer performs the single bounded hop to a remote owner. When nil the
	// node must be the owner of everything it serves (enforced by New when
	// Router is set).
	Peer PeerClient
	// PeerTimeout bounds one peer hop; required exactly when Peer is set.
	PeerTimeout time.Duration
	// BudgetGuard blocks every upstream provider call while the cluster
	// budget invariant (sum of node hard shares <= safe user budget) is
	// violated or a live provider observation has shrunk the budget. Nil
	// disables the guard (tests and single-node Phase 1 mode).
	BudgetGuard *budget.Guard
	// LeaseGate enforces the Phase 6 "at most one upstream-owner per SeriesKey"
	// invariant before any provider call: a node may call the provider only
	// while it holds a valid fencing lease. Nil disables the gate, leaving the
	// deterministic (manual) ownership path exactly as before — this is the
	// fail-closed default and the rollback path.
	LeaseGate *lease.Gate
	// BatchFanOut bounds parallel items in GetBatch; 0 selects the default.
	BatchFanOut int
	// PublicationGrace shifts the refresh-epoch boundary: a bar that closed
	// less than grace ago is still accounted to the previous epoch, so the
	// gateway does not hammer the provider before it typically publishes the
	// freshly closed candle. 0 disables the shift.
	PublicationGrace time.Duration
	// MaxRefreshAttemptsPerEpoch bounds synchronous refresh invocations per
	// series per refresh epoch (0 selects the default of 2). Once exhausted,
	// requests are served from cache with honest stale semantics until the
	// next epoch, so broken/publishing-lagged providers cannot burn the
	// budget with one call per client poll.
	MaxRefreshAttemptsPerEpoch int
	// BackgroundRefresh enables the bounded stale-while-revalidate queue: on
	// admission rejection the (admissibly) served stale response is followed
	// by exactly one deduplicated background refresh per series, so recovery
	// does not depend on client retries (task 109).
	BackgroundRefresh bool
	// BackgroundQueueCapacity bounds the stale-while-revalidate queue;
	// 0 selects the default of 64. Enqueues beyond the bound are dropped and
	// counted — never unbounded.
	BackgroundQueueCapacity int
	// Logger and Recorder are optional; a nil Logger disables decision logs and
	// a nil Recorder disables metric recording without changing behavior.
	Logger   *slog.Logger
	Recorder *observability.Recorder
}

// Router reports deterministic series ownership for the current routing
// version. cluster.Membership implements it.
type Router interface {
	IsLocalOwner(key model.SeriesKey) (bool, error)
	OwnerOf(key model.SeriesKey) (string, error)
}

// PeerClient performs the one-hop peer fetches to remote owners.
// cluster.Client implements it.
type PeerClient interface {
	PeerFetch(ctx context.Context, request model.CandleRequest) (cluster.OwnedResult, error)
	PeerBatchFetch(ctx context.Context, requests []model.CandleRequest) (cluster.OwnedBatchResult, error)
}

type Service struct {
	cache     *cache.Cache
	store     *storage.Store
	provider  provider.Provider
	scheduler *ratelimit.Scheduler
	cfg       Config
	now       func() time.Time

	persistentHits atomic.Uint64
	replicaHits    atomic.Uint64
	staleHits      atomic.Uint64

	// Task 109 cache-first state. epochs tracks per-series refresh-epoch
	// accounting (closed-bar boundary shifted by publication grace) scoped to
	// each missing range, so polling between boundaries cannot create
	// unbounded provider calls while overlapping historical requests still
	// fetch only what they are missing; the swr queue is the bounded
	// stale-while-revalidate recovery path.
	epochMu         sync.Mutex
	epochs          map[string]*epochState
	swr             *refreshQueue
	refreshQueued   atomic.Uint64
	refreshDropped  atomic.Uint64
	refreshDeferred atomic.Uint64
	warmupCompleted atomic.Uint64
	warmupSkipped   atomic.Uint64

	// fetchTimes tracks the newest durable fetch timestamp per series so
	// fresh results can carry provenance (age visibility) without widening
	// the cache contract.
	fetchTimesMu sync.RWMutex
	fetchTimes   map[string]int64
}

// epochState is the per-series refresh-epoch accounting. Attempts are scoped
// to the exact missing range fetched, so window extensions and newly settled
// gaps always fetch, while identical re-polls of the same gap are bounded.
// The map is advisory budget protection: clearing it only re-permits
// refreshes (safe direction).
type epochState struct {
	epochMS  int64
	attempts map[string]int
	lastErr  error
}

func New(cache *cache.Cache, store *storage.Store, upstream provider.Provider, scheduler *ratelimit.Scheduler, cfg Config) (*Service, error) {
	if cache == nil || store == nil || upstream == nil || scheduler == nil || cfg.RequestTimeout <= 0 || cfg.MaxStaleAge < 0 {
		return nil, fmt.Errorf("service dependencies and request timeout are required")
	}
	if cfg.Router != nil && cfg.Peer == nil {
		return nil, fmt.Errorf("a router without a peer client cannot serve remote-owned series")
	}
	if cfg.Router == nil && cfg.Peer != nil {
		return nil, fmt.Errorf("a peer client without a router has no ownership view")
	}
	if cfg.Peer != nil && cfg.PeerTimeout <= 0 {
		return nil, fmt.Errorf("peer timeout must be positive when a peer client is configured")
	}
	s := &Service{cache: cache, store: store, provider: upstream, scheduler: scheduler, cfg: cfg, now: time.Now, fetchTimes: make(map[string]int64), epochs: make(map[string]*epochState)}
	if cfg.BackgroundRefresh {
		s.swr = newRefreshQueue(s, cfg.BackgroundQueueCapacity)
	}
	return s, nil
}

// Stop terminates the background stale-while-revalidate workers. Call it
// before Scheduler.Drain during shutdown so queued recovery work stops
// submitting new provider jobs.
func (s *Service) Stop() {
	if s.swr != nil {
		s.swr.stop()
	}
}

// GetOwned serves a request the peer boundary has already verified this node
// owns. It is the owner side of the one-hop contract: memory/persistent
// coverage, one flight, bounded provider work — and never a peer call, which
// is what makes loops structurally impossible. The work is bounded by the
// configured request timeout so a dead source node cannot pin resources.
func (s *Service) GetOwned(request model.CandleRequest) (cluster.OwnedResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.RequestTimeout)
	defer cancel()
	result, err := s.Get(ctx, "peer-owner", ratelimit.PriorityLiveRefresh, request)
	if err != nil {
		return cluster.OwnedResult{}, err
	}
	return cluster.OwnedResult{Candles: result.Candles, SourceFetchedAtUTCMS: result.SourceFetchedAtUTCMS}, nil
}

// GetOwnedBatch serves a peer sub-batch of owned series positionally through
// GetOwned, with bounded parallelism and per-item results. A failing item
// never fails its siblings; results never recurse into peer calls.
func (s *Service) GetOwnedBatch(requests []model.CandleRequest) []cluster.OwnedBatchItem {
	results := make([]cluster.OwnedBatchItem, len(requests))
	fanOut := s.cfg.BatchFanOut
	if fanOut <= 0 {
		fanOut = defaultBatchFanOut
	}
	tokens := make(chan struct{}, fanOut)
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			tokens <- struct{}{}
			defer func() { <-tokens }()
			result, err := s.GetOwned(requests[index])
			results[index] = cluster.OwnedBatchItem{Result: result, Error: err}
		}(i)
	}
	wg.Wait()
	return results
}

// Get follows the fixed local path: memory coverage, SQLite coverage, one
// SeriesKey flight, bounded provider work, merge, then one atomic durable save.
// Stale data is considered only after an actual provider/scheduler failure.
// A series owned by a remote node is served through one bounded peer hop.
func (s *Service) Get(ctx context.Context, clientID string, priority ratelimit.Priority, request model.CandleRequest) (Result, error) {
	if err := request.Series.Validate(); err != nil {
		return Result{}, err
	}
	if request.FromUTCMS < 0 || request.ToUTCMS <= request.FromUTCMS {
		return Result{}, fmt.Errorf("invalid request interval")
	}
	if request.Limit <= 0 {
		return Result{}, fmt.Errorf("request limit must be positive")
	}

	requested := cache.Range{FromUTCMS: request.FromUTCMS, ToUTCMS: request.ToUTCMS}
	memory, err := s.cache.Lookup(request.Series, requested)
	if err != nil {
		return Result{}, err
	}
	memory = s.epochAwareLookup(request.Series, requested, memory)
	if memory.Complete {
		s.recordCandleAge(request.Series, memory.Entry.Candles)
		s.logDebug("memory coverage complete", request.Series)
		return s.memoryResult(request.Series, memory.Entry), nil
	}

	if err := s.loadPersistent(request.Series, requested); err != nil {
		return Result{}, err
	}
	memory, err = s.cache.Lookup(request.Series, requested)
	if err != nil {
		return Result{}, err
	}
	memory = s.epochAwareLookup(request.Series, requested, memory)
	if memory.Complete {
		s.persistentHits.Add(1)
		s.recordCandleAge(request.Series, memory.Entry.Candles)
		s.logDebug("persistent coverage complete", request.Series)
		result := s.memoryResult(request.Series, memory.Entry)
		result.CacheStatus = CachePersistentHit
		return result, nil
	}

	// Cluster routing decision: only the deterministic upstream-owner may
	// call the provider. A non-owner node makes exactly one bounded peer hop
	// and never falls back to the local provider — that invariant is what
	// keeps one SeriesKey to one upstream caller cluster-wide.
	if s.cfg.Router != nil {
		local, err := s.cfg.Router.IsLocalOwner(request.Series)
		if err != nil {
			return Result{}, err
		}
		if !local {
			return s.peerFetch(ctx, request, requested)
		}
	}

	// Task 109 cache-first refresh epoch: polling between closed-bar
	// boundaries must not create provider calls beyond the bounded per-range
	// attempt budget. A deferred series is served from cache with honest
	// stale semantics — never labeled fresh.
	if !s.refreshPermitted(request.Series, memory.Missing) {
		s.refreshDeferred.Add(1)
		s.logInfo("refresh deferred until next epoch; serving cached snapshot", request.Series)
		return s.deferredResult(request, memory)
	}

	coalesced, refreshErr := s.cache.Do(ctx, request.Series, func() error {
		return s.refresh(clientID, priority, request, requested)
	})
	if refreshErr != nil {
		var upstream *upstreamFailure
		if errors.As(refreshErr, &upstream) {
			s.noteEpochOutcome(request.Series, upstream.err)
			return s.staleOrError(request, upstream.err)
		}
		// Task 109: admission rejection degrades to the honest stale/fail-
		// closed path with the typed reason, and — when enabled — exactly one
		// deduplicated bounded background refresh is queued so recovery does
		// not depend on client retries. It never becomes a retry storm: the
		// rejection itself stays typed and visible to the client.
		if IsAdmissionRejection(refreshErr) || errors.Is(refreshErr, ratelimit.ErrDraining) {
			s.noteEpochOutcome(request.Series, refreshErr)
			s.enqueueBackgroundRefresh(clientID, request, requested)
			s.logWarn("admission rejected; serving stale/fail-closed and queueing background refresh", request.Series, "reason", AdmissionReason(refreshErr))
			return s.staleOrError(request, newTypedCause(AdmissionReason(refreshErr), refreshErr))
		}
		return Result{}, refreshErr
	}
	s.noteEpochOutcome(request.Series, nil)
	memory, err = s.cache.Lookup(request.Series, requested)
	if err != nil {
		return Result{}, err
	}
	// Completeness is judged over the closed portion only: the provider is
	// never asked for the forming candle's span, so an uncovered live tail
	// is representation, not a coverage failure (task 109).
	memory = s.epochAwareLookup(request.Series, requested, memory)
	if !memory.Complete {
		if len(memory.Entry.Candles) > 0 {
			// A successful provider call may legitimately contain permanent
			// gaps: session-traded venues (MOEX shares) produce no candles
			// outside trading hours, so the coverage of a multi-day window
			// can never be contiguous. Partial-but-non-empty coverage is
			// served — never labeled fresh — while settled empty ranges are
			// marked by refresh() so they are not re-fetched forever.
			s.logWarn("serving partial provider coverage", request.Series, "reason", "settled_session_gap")
			s.recordCandleAge(request.Series, memory.Entry.Candles)
			result := s.memoryResult(request.Series, memory.Entry)
			if coalesced {
				result.CacheStatus = CacheSingleflight
			} else {
				result.CacheStatus = CacheRefreshed
			}
			result.Freshness = StaleAcceptable
			return result, nil
		}
		// No candles at all: the response is not proof of full coverage and
		// must never be labeled fresh; fail closed so monitoring sees the
		// probe as alive-but-dataless, and the next request can retry only
		// the remaining interval.
		s.recorder().RecordIntegrityRejection(request.Series.Venue, "incomplete_coverage")
		s.logError("successful provider response left coverage incomplete", request.Series, "reason", "incomplete_coverage")
		return Result{}, ErrIncompleteCoverage
	}
	s.recordCandleAge(request.Series, memory.Entry.Candles)
	result := s.memoryResult(request.Series, memory.Entry)
	if coalesced {
		result.CacheStatus = CacheSingleflight
	} else {
		result.CacheStatus = CacheRefreshed
	}
	s.logInfo("series refreshed from provider", request.Series, "coalesced", coalesced)
	return result, nil
}

// peerFetch serves a remote-owned series: exactly one bounded peer hop per
// flight, integrity validation of the owner response, and a read-only
// replica commit so later local requests (and stale fallbacks) can be served
// without crossing the cluster again. A replica never grants this node the
// right to call the provider for the series.
func (s *Service) peerFetch(ctx context.Context, request model.CandleRequest, requested cache.Range) (Result, error) {
	coalesced, err := s.cache.Do(ctx, request.Series, func() error {
		callCtx, cancel := context.WithTimeout(ctx, s.cfg.PeerTimeout)
		defer cancel()
		response, err := s.cfg.Peer.PeerFetch(callCtx, request)
		if err != nil {
			s.logWarn("peer fetch to owner failed", request.Series, "reason", peerFailure(err))
			return err
		}
		if len(response.Candles) == 0 {
			return nil
		}
		for index := range response.Candles {
			if err := response.Candles[index].Validate(); err != nil {
				s.recorder().RecordIntegrityRejection(request.Series.Venue, "invalid_peer_ohlcv")
				s.logError("peer response integrity rejected", request.Series, "reason", "invalid_peer_ohlcv")
				return fmt.Errorf("invalid candle in peer response: %w", err)
			}
		}
		coverage := candleCoverage(response.Candles)
		if err := s.store.Save(request.Series, storage.Snapshot{Candles: response.Candles, Metadata: storage.Metadata{Replica: true, SourceFetchedAtUTCMS: response.SourceFetchedAtUTCMS, Coverage: storageCoverage(coverage)}}); err != nil {
			return err
		}
		s.noteFetch(request.Series, response.SourceFetchedAtUTCMS)
		if err := s.cache.MergeCoverage(request.Series, response.Candles, coverage, true); err != nil {
			return err
		}
		s.replicaHits.Add(1)
		return nil
	})
	if err != nil {
		// A typed peer failure never degrades into a provider call; the
		// stale-replica path below is the only fallback.
		if cluster.IsPeerError(err) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return s.staleReplicaOrError(request, err)
		}
		return Result{}, err
	}
	memory, err := s.cache.Lookup(request.Series, requested)
	if err != nil {
		return Result{}, err
	}
	if !memory.Complete {
		s.recorder().RecordIntegrityRejection(request.Series.Venue, "incomplete_peer_coverage")
		s.logError("peer response left coverage incomplete", request.Series, "reason", "incomplete_peer_coverage")
		return Result{}, ErrIncompleteCoverage
	}
	s.recordCandleAge(request.Series, memory.Entry.Candles)
	result := s.memoryResult(request.Series, memory.Entry)
	if coalesced {
		result.CacheStatus = CacheSingleflight
	} else {
		result.CacheStatus = CacheRefreshed
	}
	s.logInfo("series served from remote owner", request.Series, "coalesced", coalesced)
	return result, nil
}

// staleReplicaOrError applies the stale-if-error policy to a locally cached
// replica after a peer failure. It never calls the provider.
func (s *Service) staleReplicaOrError(request model.CandleRequest, peerErr error) (Result, error) {
	snapshot, allowed, err := s.store.StaleIfUpstreamError(request.Series, request.FromUTCMS, request.ToUTCMS, s.now().UnixMilli(), s.cfg.MaxStaleAge.Milliseconds(), peerErr)
	if err != nil {
		return Result{}, err
	}
	if !allowed {
		s.logWarn("no acceptable stale replica; failing closed", request.Series, "reason", peerFailure(peerErr))
		return Result{CacheStatus: CacheMiss, Freshness: StaleRejected}, peerErr
	}
	s.staleHits.Add(1)
	if snapshot.Metadata.Replica {
		s.replicaHits.Add(1)
	}
	s.logWarn("serving stale replica after peer failure", request.Series, "reason", peerFailure(peerErr), "age_ms", s.now().UnixMilli()-snapshot.Metadata.SourceFetchedAtUTCMS)
	return Result{Candles: snapshot.Candles, CacheStatus: CacheMiss, Freshness: StaleAcceptable, Replica: true, SourceFetchedAtUTCMS: snapshot.Metadata.SourceFetchedAtUTCMS, StaleCause: peerErr}, nil
}

func (s *Service) loadPersistent(key model.SeriesKey, requested cache.Range) error {
	snapshot, found, err := s.store.Get(key, requested.FromUTCMS, requested.ToUTCMS)
	if err != nil || !found {
		return err
	}
	s.noteFetch(key, snapshot.Metadata.SourceFetchedAtUTCMS)
	coverage := make([]cache.Range, 0, len(snapshot.Metadata.Coverage))
	for _, interval := range snapshot.Metadata.Coverage {
		coverage = append(coverage, cache.Range{FromUTCMS: interval.FromUTCMS, ToUTCMS: interval.ToUTCMS})
	}
	return s.cache.MergeCoverage(key, snapshot.Candles, coverage, snapshot.Metadata.Replica)
}

func (s *Service) refresh(clientID string, priority ratelimit.Priority, request model.CandleRequest, requested cache.Range) error {
	// Phase 6 enforcement: before any upstream work, the lease gate must permit
	// this node to be the upstream-owner for the series. With no gate configured
	// (the fail-closed manual default and the rollback path) this is a no-op and
	// the deterministic routing decision already made upstream stands. When the
	// gate is configured, a missing/unexpired/fenced lease denies the provider
	// call and the request falls back to the stale/replica path — never a second
	// concurrent owner.
	if s.cfg.LeaseGate != nil {
		if d := s.cfg.LeaseGate.MayCallProvider(request.Series.Identity()); !d.Allowed {
			s.logWarn("lease gate denied upstream provider call", request.Series, "reason", d.Reason)
			return &upstreamFailure{err: d.DeniedAsError()}
		}
	}
	// Upstream work is blocked before any scheduler admission while
	// the cluster budget invariant is violated. This is a hard fail-closed
	// gate, not a warning.
	if s.cfg.BudgetGuard != nil {
		if err := s.cfg.BudgetGuard.Admit(); err != nil {
			s.logWarn("upstream blocked by cluster budget guard", request.Series, "reason", "unsafe_cluster_budget")
			return &upstreamFailure{err: err}
		}
	}
	if err := s.loadPersistent(request.Series, requested); err != nil {
		return err
	}
	lookup, err := s.cache.Lookup(request.Series, requested)
	if err != nil || lookup.Complete {
		return err
	}

	// hasRealCandles gates empty-range marking: a series that has produced
	// no candles at all keeps failing closed (visible data-less probe),
	// while gaps inside otherwise real data are honest session holes.
	hasRealCandles := len(lookup.Entry.Candles) > 0
	for _, missing := range lookup.Missing {
		// Task 109: the live edge is representation-only — ranges inside the
		// forming candle are never fetched, so one provider refresh per
		// series per epoch covers the closed bars.
		fetchRange, ok := s.trimFetchRangeToEpoch(request.Series, missing)
		if !ok {
			continue
		}
		// Task 109: the singleflight leader consumes one refresh-epoch
		// attempt for exactly this missing range, immediately before the
		// provider work. Coalesced joiners never reach this line, and
		// gateway-initiated background work (the dedicated warmup identity)
		// is bounded by its own queue/retry caps instead of the sync
		// per-epoch ledger, so recovery cannot lock client polls out.
		if clientID != warmupClientID {
			s.noteEpochFetch(request.Series, fetchRange)
		}
		fetch := request
		fetch.FromUTCMS, fetch.ToUTCMS = fetchRange.FromUTCMS, fetchRange.ToUTCMS
		providerCtx, cancel := context.WithTimeout(context.Background(), s.cfg.RequestTimeout)
		var candles []model.Candle
		var metadata model.ProviderMetadata
		started := s.now()
		s.recorder().InflightInc(request.Series.Venue, providerName)
		result, submitErr := s.scheduler.Submit(providerCtx, clientID, priority, func(opCtx context.Context) error {
			var err error
			candles, metadata, err = s.provider.FetchCandles(opCtx, fetch)
			return err
		})
		if submitErr != nil {
			cancel()
			s.recorder().InflightDec(request.Series.Venue, providerName)
			return submitErr
		}
		err := <-result
		cancel()
		s.recorder().InflightDec(request.Series.Venue, providerName)
		s.recorder().RecordUpstream(providerName, request.Series.Venue, "get_candles", upstreamStatus(err), s.now().Sub(started))
		if err != nil {
			s.logWarn("upstream fetch failed", request.Series, "reason", upstreamStatus(err))
			return &upstreamFailure{err: err}
		}
		if len(candles) == 0 {
			// The provider answered OK with no candles for this range. For
			// session-traded venues that is a permanent fact (market closed),
			// and without marking it the same empty ranges are re-fetched on
			// every request, burning the provider budget. A settled empty
			// range — closed well before the live edge so delayed upstream
			// writes cannot be missed — is marked as empty coverage in
			// memory only: absence is never persisted, a restart re-asks.
			if hasRealCandles {
				if step, ok := stepForTimeframe(request.Series.Timeframe); ok {
					settledTo := s.now().Add(-emptyRangeGraceSteps * step).UnixMilli()
					if fetchRange.ToUTCMS < settledTo {
						settledTo = fetchRange.ToUTCMS
					}
					if settledTo > fetchRange.FromUTCMS {
						settledRange := cache.Range{FromUTCMS: fetchRange.FromUTCMS, ToUTCMS: settledTo}
						if err := s.cache.MergeCoverage(request.Series, nil, []cache.Range{settledRange}, lookup.Entry.Replica); err != nil {
							return err
						}
					}
				}
			}
			continue
		}
		hasRealCandles = true
		coverage := candleCoverage(candles)
		for index := range candles {
			if err := candles[index].Validate(); err != nil {
				s.recorder().RecordIntegrityRejection(request.Series.Venue, "invalid_ohlcv")
				s.logError("candle integrity rejected before cache", request.Series, "reason", "invalid_ohlcv")
				return &upstreamFailure{err: fmt.Errorf("invalid candle in upstream response: %w", err)}
			}
		}
		if err := s.store.Save(request.Series, storage.Snapshot{Candles: candles, Metadata: storage.Metadata{SourceFetchedAtUTCMS: s.now().UnixMilli(), Coverage: storageCoverage(coverage)}}); err != nil {
			return err
		}
		s.noteFetch(request.Series, s.now().UnixMilli())
		if err := s.cache.MergeCoverage(request.Series, candles, coverage, false); err != nil {
			return err
		}
		if metadata.RateLimitRemaining >= 0 {
			s.logDebug("upstream rate budget observed", request.Series, "remaining", metadata.RateLimitRemaining)
			// A provider-reported remaining budget feeds the cluster
			// guard's observed view; a shrink below the configured sum
			// latches upstream work closed until restart with a corrected
			// configuration.
			if s.cfg.BudgetGuard != nil && metadata.RateLimitRemaining > 0 {
				s.cfg.BudgetGuard.ObserveShrink(int(metadata.RateLimitRemaining))
			}
		}
	}
	return nil
}

func (s *Service) staleOrError(request model.CandleRequest, upstreamErr error) (Result, error) {
	snapshot, allowed, err := s.store.StaleIfUpstreamError(request.Series, request.FromUTCMS, request.ToUTCMS, s.now().UnixMilli(), s.cfg.MaxStaleAge.Milliseconds(), upstreamErr)
	if err != nil {
		return Result{}, err
	}
	if !allowed {
		s.logWarn("no acceptable stale fallback; failing closed", request.Series, "reason", upstreamStatus(upstreamErr))
		return Result{CacheStatus: CacheMiss, Freshness: StaleRejected}, upstreamErr
	}
	s.staleHits.Add(1)
	if snapshot.Metadata.Replica {
		s.replicaHits.Add(1)
	}
	s.logWarn("serving stale fallback after upstream failure", request.Series, "reason", upstreamStatus(upstreamErr), "age_ms", s.now().UnixMilli()-snapshot.Metadata.SourceFetchedAtUTCMS)
	return Result{Candles: snapshot.Candles, CacheStatus: CacheMiss, Freshness: StaleAcceptable, Replica: snapshot.Metadata.Replica, SourceFetchedAtUTCMS: snapshot.Metadata.SourceFetchedAtUTCMS, StaleCause: upstreamErr}, nil
}

func (s *Service) memoryResult(key model.SeriesKey, entry cache.Entry) Result {
	if entry.Replica {
		s.replicaHits.Add(1)
	}
	return Result{Candles: entry.Candles, CacheStatus: CacheMemoryHit, Freshness: Fresh, Replica: entry.Replica, SourceFetchedAtUTCMS: s.recordedFetchTime(key)}
}

// ---------------------------------------------------------------------------
// Task 109 — cache-first refresh epochs, typed degradation, stale-while-
// revalidate, and dosed warmup.
// ---------------------------------------------------------------------------

// maxTrackedEpochs bounds the per-series epoch map defensively. Clearing it
// only re-permits refreshes — the safe failure direction.
const maxTrackedEpochs = 8192

// maxTrackedRangeSignatures bounds the per-series range-attempt map
// defensively; clearing re-permits refreshes (safe direction).
const maxTrackedRangeSignatures = 64

// defaultMaxRefreshAttemptsPerEpoch is the bounded number of synchronous
// refresh invocations per series per epoch when the configuration does not
// select one. One attempt covers the normal closed-bar refresh; the second
// absorbs provider publication lag before the honest deferred-stale state.
const defaultMaxRefreshAttemptsPerEpoch = 2

// refreshEpochMS returns the closed-bar boundary that governs refresh
// accounting for a series at time now: the current bar's open time, shifted
// back by the provider publication grace so a freshly closed bar is not
// hammered before the provider typically publishes it.
func refreshEpochMS(now time.Time, step, grace time.Duration) int64 {
	stepMS := step.Milliseconds()
	if stepMS <= 0 {
		return 0
	}
	t := now.Add(-grace).UnixMilli()
	return (t / stepMS) * stepMS
}

// missingRangeSignature is the bounded accounting key of one missing range:
// identical missing ranges poll identically and are therefore bounded
// together; different ranges always fetch independently.
func missingRangeSignature(r cache.Range) string {
	return strconv.FormatInt(r.FromUTCMS, 36) + ":" + strconv.FormatInt(r.ToUTCMS, 36)
}

func (s *Service) epochGrace() time.Duration { return s.cfg.PublicationGrace }

func (s *Service) maxEpochAttempts() int {
	if s.cfg.MaxRefreshAttemptsPerEpoch > 0 {
		return s.cfg.MaxRefreshAttemptsPerEpoch
	}
	return defaultMaxRefreshAttemptsPerEpoch
}

// refreshPermitted reports whether fetching the listed missing ranges of the
// series may proceed in the current refresh epoch. A range without budget
// left is served from cache with honest stale semantics until the next epoch.
func (s *Service) refreshPermitted(key model.SeriesKey, missing []cache.Range) bool {
	step, ok := stepForTimeframe(key.Timeframe)
	if !ok {
		return true
	}
	epoch := refreshEpochMS(s.now(), step, s.epochGrace())
	s.epochMu.Lock()
	defer s.epochMu.Unlock()
	state := s.epochs[key.Identity()]
	if state == nil || state.epochMS != epoch {
		return true
	}
	for _, r := range missing {
		if state.attempts[missingRangeSignature(r)] < s.maxEpochAttempts() {
			return true
		}
	}
	return false
}

// noteEpochFetch consumes one attempt for the exact missing range about to be
// fetched. Called only from the singleflight leader, so coalesced joiners
// never consume range budget.
func (s *Service) noteEpochFetch(key model.SeriesKey, missing cache.Range) {
	step, ok := stepForTimeframe(key.Timeframe)
	if !ok {
		return
	}
	s.epochMu.Lock()
	defer s.epochMu.Unlock()
	if len(s.epochs) > maxTrackedEpochs {
		s.epochs = make(map[string]*epochState)
	}
	epoch := refreshEpochMS(s.now(), step, s.epochGrace())
	identity := key.Identity()
	state := s.epochs[identity]
	if state == nil || state.epochMS != epoch {
		state = &epochState{epochMS: epoch, attempts: make(map[string]int)}
	}
	if state.attempts == nil {
		state.attempts = make(map[string]int)
	}
	if len(state.attempts) > maxTrackedRangeSignatures {
		state.attempts = make(map[string]int)
	}
	state.attempts[missingRangeSignature(missing)]++
	s.epochs[identity] = state
}

// noteEpochOutcome records the last refresh outcome for the series' current
// epoch so deferred/fail-closed responses can carry the honest cause.
func (s *Service) noteEpochOutcome(key model.SeriesKey, err error) {
	step, ok := stepForTimeframe(key.Timeframe)
	if !ok {
		return
	}
	s.epochMu.Lock()
	defer s.epochMu.Unlock()
	epoch := refreshEpochMS(s.now(), step, s.epochGrace())
	identity := key.Identity()
	state := s.epochs[identity]
	if state == nil || state.epochMS != epoch {
		state = &epochState{epochMS: epoch, attempts: make(map[string]int)}
	}
	state.lastErr = err
	s.epochs[identity] = state
}

// epochLastErr returns the most recent refresh error for the series' current
// epoch, if any.
func (s *Service) epochLastErr(key model.SeriesKey) error {
	step, ok := stepForTimeframe(key.Timeframe)
	if !ok {
		return nil
	}
	s.epochMu.Lock()
	defer s.epochMu.Unlock()
	epoch := refreshEpochMS(s.now(), step, s.epochGrace())
	state := s.epochs[key.Identity()]
	if state == nil || state.epochMS != epoch {
		return nil
	}
	return state.lastErr
}

// epochAwareLookup evaluates completeness over the CLOSED portion of the
// requested window: ranges at/after the current forming candle's open are
// representation-only (the provider may not publish the forming candle), so
// they never force a refresh and never block a cache hit. The returned
// candles still cover the full requested window, so include_incomplete
// clients keep their cached live edge row (task 109).
func (s *Service) epochAwareLookup(key model.SeriesKey, requested cache.Range, full cache.LookupResult) cache.LookupResult {
	step, ok := stepForTimeframe(key.Timeframe)
	if !ok {
		return full
	}
	boundary := (s.now().UnixMilli() / step.Milliseconds()) * step.Milliseconds()
	if requested.ToUTCMS <= boundary {
		return full // window is fully closed: legacy behavior
	}
	if requested.FromUTCMS >= boundary {
		// Window lies inside the forming candle: no closed candles are
		// required. Cached in-window data (best-effort live edge) satisfies
		// the request; otherwise the refresh path runs and fails closed
		// when the provider publishes nothing (task 109 invariant).
		if len(full.Entry.Candles) > 0 {
			full.Complete = true
			full.Missing = nil
		}
		return full
	}
	closedReq := cache.Range{FromUTCMS: requested.FromUTCMS, ToUTCMS: boundary}
	closed, err := s.cache.Lookup(key, closedReq)
	if err != nil {
		return full
	}
	full.Missing = closed.Missing
	full.Complete = closed.Complete
	return full
}

// trimFetchRangeToEpoch clips one missing range to the closed portion of the
// current epoch. ok=false means the range carries no closed candles (live
// edge only) and must not be fetched.
func (s *Service) trimFetchRangeToEpoch(key model.SeriesKey, missing cache.Range) (cache.Range, bool) {
	step, ok := stepForTimeframe(key.Timeframe)
	if !ok {
		return missing, true
	}
	boundary := (s.now().UnixMilli() / step.Milliseconds()) * step.Milliseconds()
	if missing.ToUTCMS <= boundary {
		return missing, true
	}
	if missing.FromUTCMS >= boundary {
		return cache.Range{}, false
	}
	trimmed := cache.Range{FromUTCMS: missing.FromUTCMS, ToUTCMS: boundary}
	if trimmed.ToUTCMS <= trimmed.FromUTCMS {
		return cache.Range{}, false
	}
	return trimmed, true
}

// deferredResult serves a series whose refresh attempts are exhausted for the
// current epoch. Cached candles are served with honest stale semantics and a
// typed cause; a series without any cached candle stays fail-closed with the
// last refresh outcome.
func (s *Service) deferredResult(request model.CandleRequest, lookup cache.LookupResult) (Result, error) {
	if len(lookup.Entry.Candles) == 0 {
		err := s.epochLastErr(request.Series)
		if err == nil {
			err = ErrIncompleteCoverage
		}
		s.recorder().RecordIntegrityRejection(request.Series.Venue, "refresh_deferred_no_data")
		return s.staleOrError(request, err)
	}
	s.recordCandleAge(request.Series, lookup.Entry.Candles)
	result := s.memoryResult(request.Series, lookup.Entry)
	result.Freshness = StaleAcceptable
	result.StaleCause = newTypedCause("refresh_deferred", errRefreshDeferred)
	return result, nil
}

// errRefreshDeferred is the typed cause for cache-served responses whose
// refresh attempts are exhausted for the current epoch.
var errRefreshDeferred = errors.New("refresh deferred until next epoch")

// typedCause marks a degraded response cause with a bounded code while
// preserving error unwrapping: admission sentinel checks (HTTP 429 contract)
// keep working through the wrap.
type typedCause struct {
	code string
	err  error
}

func newTypedCause(code string, err error) *typedCause { return &typedCause{code: code, err: err} }

func (e *typedCause) Error() string { return e.err.Error() }
func (e *typedCause) Unwrap() error { return e.err }

// TypedCauseCode returns the bounded cause code carried by a typedCause
// (e.g. "refresh_deferred" or an admission reason), or "" when the error is
// not a typed cause. It lets HTTP layers label degraded responses without
// string parsing.
func TypedCauseCode(err error) string {
	var tc *typedCause
	if errors.As(err, &tc) {
		return tc.code
	}
	return ""
}

// IsAdmissionRejection reports whether the error is a bounded scheduler
// admission rejection. Draining is excluded: queueing background recovery
// while draining is pointless.
func IsAdmissionRejection(err error) bool {
	return errors.Is(err, ratelimit.ErrQueueFull) || errors.Is(err, ratelimit.ErrClientQuota) ||
		errors.Is(err, ratelimit.ErrNodeBudget) || errors.Is(err, ratelimit.ErrClientTrackingCapacity)
}

// AdmissionReason maps an admission/draining error to the bounded typed
// reason vocabulary shared by JSON responses, logs, and Prometheus.
func AdmissionReason(err error) string {
	switch {
	case errors.Is(err, ratelimit.ErrNodeBudget):
		return "node_budget"
	case errors.Is(err, ratelimit.ErrClientQuota):
		return "client_quota"
	case errors.Is(err, ratelimit.ErrQueueFull), errors.Is(err, ratelimit.ErrClientTrackingCapacity):
		return "queue_full"
	case errors.Is(err, ratelimit.ErrDraining):
		return "draining"
	default:
		return ""
	}
}

// warmupClientID is the logical identity of gateway-initiated background
// work (stale-while-revalidate and prewarm). A dedicated identity keeps its
// quota consumption out of the trading clients' per-client quota.
const warmupClientID = "warmup"

// enqueueBackgroundRefresh places exactly one deduplicated bounded refresh for
// the series on the stale-while-revalidate queue. It is a no-op unless the
// background path is enabled; after Stop (drain) the queue refuses work and
// Scheduler admission rejects, so shutdown never creates new provider jobs.
// The job runs under the dedicated warmup identity so backpressure recovery
// never consumes a trading client's quota.
func (s *Service) enqueueBackgroundRefresh(clientID string, request model.CandleRequest, requested cache.Range) {
	if s.swr == nil {
		return
	}
	if !s.swr.enqueue(swrJob{clientID: warmupClientID, request: request, requested: requested}) {
		s.refreshDropped.Add(1)
		return
	}
	s.refreshQueued.Add(1)
}

// WarmupOne performs one dosed warmup refresh at warmup priority. A series
// whose requested window is already covered is skipped without any provider
// call or budget consumption. It bypasses the per-epoch synchronous gate —
// the warmup is the controlled recovery path — but remains fully subject to
// scheduler admission and the cluster budget guard.
func (s *Service) WarmupOne(request model.CandleRequest) (covered bool, err error) {
	if err := request.Series.Validate(); err != nil {
		return false, err
	}
	if request.FromUTCMS < 0 || request.ToUTCMS <= request.FromUTCMS || request.Limit <= 0 {
		return false, fmt.Errorf("invalid warmup request interval")
	}
	requested := cache.Range{FromUTCMS: request.FromUTCMS, ToUTCMS: request.ToUTCMS}
	// The warmup's completeness horizon is the settled past (2 timeframe
	// steps back, mirroring the empty-range settle rule): market-closed /
	// not-yet-published trailing hours are never required, so the night
	// patrol skips fully settled series instead of re-fetching provably
	// empty ranges forever. Live client serves keep the strict boundary.
	horizon := s.warmupHorizon(request.Series)
	horizonReq := requested
	if horizon > 0 && horizon < requested.ToUTCMS {
		horizonReq = cache.Range{FromUTCMS: requested.FromUTCMS, ToUTCMS: horizon}
		if horizonReq.ToUTCMS <= horizonReq.FromUTCMS {
			s.warmupSkipped.Add(1)
			return true, nil
		}
	}
	if memory, err := s.cache.Lookup(request.Series, horizonReq); err == nil && memory.Complete {
		s.warmupSkipped.Add(1)
		return true, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.RequestTimeout)
	defer cancel()
	if _, err := s.cache.Do(ctx, request.Series, func() error {
		return s.refresh(warmupClientID, ratelimit.PriorityWarmup, request, requested)
	}); err != nil {
		return false, err
	}
	memory, err := s.cache.Lookup(request.Series, horizonReq)
	if err != nil {
		return false, err
	}
	if memory.Complete {
		s.warmupCompleted.Add(1)
		s.logInfo("warmup series covered", request.Series)
	} else {
		s.logWarn("warmup fetch left settled coverage incomplete", request.Series)
	}
	return memory.Complete, nil
}

// warmupHorizon returns the settled-past boundary (now - 2 timeframe steps)
// for the series, or 0 when the timeframe is unknown.
func (s *Service) warmupHorizon(key model.SeriesKey) int64 {
	step, ok := stepForTimeframe(key.Timeframe)
	if !ok {
		return 0
	}
	return s.now().UnixMilli() - 2*step.Milliseconds()
}

// recordedFetchTime returns the durable fetch timestamp recorded for a
// series identity.
func (s *Service) recordedFetchTime(key model.SeriesKey) int64 {
	s.fetchTimesMu.RLock()
	defer s.fetchTimesMu.RUnlock()
	return s.fetchTimes[key.Identity()]
}

// noteFetch records the newest durable fetch timestamp for a series
// identity so later fresh responses carry provenance.
func (s *Service) noteFetch(key model.SeriesKey, fetchedAtUTCMS int64) {
	identity := key.Identity()
	if identity == "" || fetchedAtUTCMS <= 0 {
		return
	}
	s.fetchTimesMu.Lock()
	if fetchedAtUTCMS >= s.fetchTimes[identity] {
		s.fetchTimes[identity] = fetchedAtUTCMS
	}
	s.fetchTimesMu.Unlock()
}

func (s *Service) Metrics() Metrics {
	cacheMetrics := s.cache.Metrics()
	return Metrics{
		MemoryHits:        cacheMetrics.Hits,
		MemoryMisses:      cacheMetrics.Misses,
		SingleflightJoins: cacheMetrics.Coalesced,
		PersistentHits:    s.persistentHits.Load(),
		ReplicaResponses:  s.replicaHits.Load(),
		StaleResponses:    s.staleHits.Load(),
		RefreshQueued:     s.refreshQueued.Load(),
		RefreshDropped:    s.refreshDropped.Load(),
		RefreshDeferred:   s.refreshDeferred.Load(),
		WarmupCompleted:   s.warmupCompleted.Load(),
		WarmupSkipped:     s.warmupSkipped.Load(),
	}
}

// Recorder exposes the metric recorder for the HTTP metrics endpoint. It
// returns nil when observability was not configured.
func (s *Service) Recorder() *observability.Recorder { return s.cfg.Recorder }

// SchedulerSnapshot exposes bounded admission gauges for the metrics endpoint.
func (s *Service) SchedulerSnapshot() ratelimit.Snapshot { return s.scheduler.Snapshot() }

// SchedulerNextAllowedAt exposes the next admissible provider attempt time so
// HTTP Retry-After hints are computed from the real accounting window.
func (s *Service) SchedulerNextAllowedAt() time.Time { return s.scheduler.NextAllowedAt() }

// BudgetGuard exposes the cluster budget guard for the metrics endpoint; it
// returns nil when the guard is not configured (single-node Phase 1 mode).
func (s *Service) BudgetGuard() *budget.Guard { return s.cfg.BudgetGuard }

// providerName is the fixed label vocabulary for the upstream provider; it is
// deliberately not derived from configuration or error text.
const providerName = "tbank"

// emptyRangeGraceSteps is how many candle steps past a range's end the
// provider is given to publish delayed candles before a successful empty
// response may be trusted as "no candles exist here".
const emptyRangeGraceSteps = 2

// stepForTimeframe maps the bounded client timeframe vocabulary to the candle
// step. ok=false (unknown timeframe) disables empty-range marking.
func stepForTimeframe(timeframe string) (time.Duration, bool) {
	switch timeframe {
	case "1m":
		return time.Minute, true
	case "5m":
		return 5 * time.Minute, true
	case "15m":
		return 15 * time.Minute, true
	case "30m":
		return 30 * time.Minute, true
	case "1h":
		return time.Hour, true
	case "4h":
		return 4 * time.Hour, true
	case "1d":
		return 24 * time.Hour, true
	default:
		return 0, false
	}
}

func (s *Service) recorder() *observability.Recorder {
	if s.cfg.Recorder != nil {
		return s.cfg.Recorder
	}
	return disabledRecorder
}

// disabledRecorder is the shared no-op target; writes to it are dropped.
var disabledRecorder = observability.NewRecorder()

func (s *Service) logDebug(message string, key model.SeriesKey, args ...any) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Log(context.Background(), slog.LevelDebug, message, seriesLogAttrs(key, args)...)
	}
}
func (s *Service) logInfo(message string, key model.SeriesKey, args ...any) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Log(context.Background(), slog.LevelInfo, message, seriesLogAttrs(key, args)...)
	}
}
func (s *Service) logWarn(message string, key model.SeriesKey, args ...any) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Log(context.Background(), slog.LevelWarn, message, seriesLogAttrs(key, args)...)
	}
}
func (s *Service) logError(message string, key model.SeriesKey, args ...any) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Log(context.Background(), slog.LevelError, message, seriesLogAttrs(key, args)...)
	}
}

// seriesLogAttrs keeps log attributes on the bounded venue/timeframe
// vocabulary; raw symbols are never logged.
func seriesLogAttrs(key model.SeriesKey, args []any) []any {
	attrs := []any{"venue", key.Venue, "market_type", key.MarketType, "timeframe", key.Timeframe}
	return append(attrs, args...)
}

// recordCandleAge updates the market-age gauge from the newest closed candle.
func (s *Service) recordCandleAge(key model.SeriesKey, candles []model.Candle) {
	if s.cfg.Recorder == nil {
		return
	}
	var newestClosed int64
	for _, candle := range candles {
		if candle.IsClosed && candle.CloseTimeUTCMS > newestClosed {
			newestClosed = candle.CloseTimeUTCMS
		}
	}
	if newestClosed > 0 {
		s.cfg.Recorder.RecordCandleAge(key.Venue, key.Timeframe, s.now().Sub(time.UnixMilli(newestClosed)).Seconds())
	}
}

// upstreamStatus maps an upstream error to a bounded status vocabulary.
func upstreamStatus(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var typed *apperror.Error
	if errors.As(err, &typed) {
		return strings.ToLower(string(typed.Code))
	}
	return "upstream_error"
}

// peerFailure maps a peer-boundary error to the same bounded vocabulary used
// for upstream failures; it exists to keep peer statuses out of the upstream
// metric family while remaining log-safe.
func peerFailure(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "peer_deadline_exceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var typed *apperror.Error
	if errors.As(err, &typed) {
		return strings.ToLower(string(typed.Code))
	}
	return "peer_error"
}

func candleCoverage(candles []model.Candle) []cache.Range {
	coverage := make([]cache.Range, 0, len(candles))
	for _, candle := range candles {
		coverage = append(coverage, cache.Range{FromUTCMS: candle.OpenTimeUTCMS, ToUTCMS: candle.CloseTimeUTCMS})
	}
	return coverage
}

func storageCoverage(coverage []cache.Range) []storage.Interval {
	result := make([]storage.Interval, 0, len(coverage))
	for _, interval := range coverage {
		result = append(result, storage.Interval{FromUTCMS: interval.FromUTCMS, ToUTCMS: interval.ToUTCMS})
	}
	return result
}

func IsOverloaded(err error) bool {
	return errors.Is(err, ratelimit.ErrQueueFull) || errors.Is(err, ratelimit.ErrClientQuota) || errors.Is(err, ratelimit.ErrNodeBudget) || errors.Is(err, ratelimit.ErrClientTrackingCapacity) || errors.Is(err, ratelimit.ErrDraining)
}

type upstreamFailure struct{ err error }

func (e *upstreamFailure) Error() string { return e.err.Error() }
func (e *upstreamFailure) Unwrap() error { return e.err }
