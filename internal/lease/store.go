// Package lease implements the Phase 6 automatic-failover control plane: a
// per-SeriesKey fencing-token lease store with quorum (witness) confirmation
// and deterministic standby takeover, plus the gate that enforces "at most one
// upstream-owner per SeriesKey" before any provider call.
//
// Safety model (spec Gate 6):
//   - The deterministic primary (weighted rendezvous) is the normal owner and,
//     in manual failover mode, the ONLY node allowed to call the provider.
//   - In automatic mode a standby may take over a series only by acquiring a
//     lease whose fencing token is strictly greater than the last committed
//     token, confirmed by a strict majority of witnesses. A node whose held
//     token is below the committed token (a presumed-dead/stale owner) is
//     rejected and may not call the provider — this is what prevents
//     split-brain under crash or partition.
//   - If the lease store (witness quorum) is unreachable the gate fails closed:
//     no valid lease -> no provider call. Degradation never widens ownership.
package lease

import (
	"errors"
	"sync"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
)

// Token is a monotonic fencing token, unique per SeriesKey. A greater token is
// always newer; it is issued only by a successful quorum commit.
type Token uint64

// State is the committed cluster view of one series' lease.
type State struct {
	Owner     string
	Token     Token
	Version   int // routing_version under which the lease was issued
	ExpiresAt int64
	IssuedAt  int64
}

// Lease is a node's locally held ownership right for a series.
type Lease struct {
	Holder    string
	Token     Token
	Version   int
	ExpiresAt int64
	IssuedAt  int64
}

// Errors returned by the lease control plane. They are typed so the data plane
// and runbooks can branch on them.
var (
	ErrLeaseConflict     = apperror.New(apperror.CodeLeaseConflict, "series already has an unexpired owner", 0)
	ErrLeaseStale        = apperror.New(apperror.CodeLeaseExpired, "proposed token is not newer than committed token", 0)
	ErrLeaseExpired      = apperror.New(apperror.CodeLeaseExpired, "no unexpired lease held", 0)
	ErrLeaseUnavailable  = apperror.New(apperror.CodeLeaseUnavailable, "lease store unreachable", 0)
	ErrLeaseQuorumFailed = apperror.New(apperror.CodeLeaseQuorumFailed, "lease quorum not reached", 0)
	ErrLeaseManualMode   = apperror.New(apperror.CodeLeaseManualMode, "automatic takeover disabled in manual failover mode", 0)
)

// Member is one quorum participant the coordinator drives. It is satisfied by
// NodeStore (the local self-vote) and by the real peer transport to remote
// witnesses. Acquire is atomic (propose+commit) so a concurrent candidate on the
// same member is resolved by fencing alone.
type Member interface {
	// Peek is read-only: the member's current view of the series lease.
	Peek(seriesKey string) State
	// Acquire commits the lease if fencing passes (token strictly greater than
	// the stored token and no live conflicting owner). It returns the resulting
	// state or a typed conflict/stale error.
	Acquire(seriesKey string, holder string, version int, ttlMS int64, token Token) (State, error)
}

// NodeStore is a single node's local lease memory: the leases it currently
// holds and the last committed state it has observed for each series. The
// committed truth lives on the witnesses; this is the node's working copy and,
// with itself counted as a quorum vote, also one member.
type NodeStore struct {
	mu       sync.RWMutex
	held     map[string]Lease
	observed map[string]State
	nodeID   string
	now      func() time.Time
}

// NewNodeStore builds an empty store for nodeID. now defaults to time.Now.
func NewNodeStore(nodeID string, now func() time.Time) *NodeStore {
	if now == nil {
		now = time.Now
	}
	return &NodeStore{held: make(map[string]Lease), observed: make(map[string]State), nodeID: nodeID, now: now}
}

// Held returns the unexpired lease this node holds for key, if any. An expired
// lease is reported as not-held so the gate cannot act on a stale right.
func (s *NodeStore) Held(key string) (Lease, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l, ok := s.held[key]
	if !ok {
		return Lease{}, false
	}
	if s.now().UnixMilli() >= l.ExpiresAt {
		return Lease{}, false
	}
	return l, true
}

// Observed returns the last committed state this node has seen for key.
func (s *NodeStore) Observed(key string) State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.observed[key]
}

// Peek reports the observed committed view (read-only quorum member).
func (s *NodeStore) Peek(seriesKey string) State { return s.Observed(seriesKey) }

// Acquire records the lease on the observed truth copy if fencing passes. It
// does not set the locally held lease; the coordinator only marks a lease held
// once a quorum is won, which stops a node that lost the race from acting on a
// provisionally committed token.
func (s *NodeStore) Acquire(seriesKey string, holder string, version int, ttlMS int64, token Token) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.observed[seriesKey]
	if token <= cur.Token {
		return cur, ErrLeaseStale
	}
	if cur.Owner != "" && cur.Owner != holder && s.now().UnixMilli() < cur.ExpiresAt {
		return cur, ErrLeaseConflict
	}
	issued := s.now().UnixMilli()
	ns := State{Owner: holder, Token: token, Version: version, ExpiresAt: issued + ttlMS, IssuedAt: issued}
	s.observed[seriesKey] = ns
	return ns, nil
}

func (s *NodeStore) setHeld(key string, l Lease) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.held[key] = l
}

// dropHeld removes a locally held lease. Used when a renewal/quorum fails or a
// node discovers it was fenced, so the gate immediately stops the provider right.
func (s *NodeStore) dropHeld(key string) {
	s.mu.Lock()
	delete(s.held, key)
	s.mu.Unlock()
}

// HeldKeys returns the series keys this node currently holds a lease for. Used
// by the background renewer to confirm liveness on a strict-majority cadence.
func (s *NodeStore) HeldKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.held))
	for k := range s.held {
		keys = append(keys, k)
	}
	return keys
}

func (s *NodeStore) setObserved(key string, st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.observed[key]
	// Never let an older token overwrite a newer observed token.
	if st.Token >= cur.Token {
		s.observed[key] = st
	}
}

// WitnessStore is the source of truth for committed leases. In production each
// witness runs one; the candidate collects a strict-majority Acquire before it
// may treat a lease as won. Every Acquire enforces fencing: accepted only if its
// token is strictly greater than the stored token, and an unexpired lease held
// by another node is never overwritten.
type WitnessStore struct {
	mu     sync.Mutex
	states map[string]State
	now    func() time.Time
}

// NewWitnessStore builds an empty witness store. now defaults to time.Now.
func NewWitnessStore(now func() time.Time) *WitnessStore {
	if now == nil {
		now = time.Now
	}
	return &WitnessStore{states: make(map[string]State), now: now}
}

// Peek is read-only: the committed view for key.
func (w *WitnessStore) Peek(seriesKey string) State {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.states[seriesKey]
}

// Acquire records a lease if token > stored token (fencing) and no unexpired
// conflicting owner exists. It returns the resulting state or a typed error.
func (w *WitnessStore) Acquire(seriesKey string, holder string, version int, ttlMS int64, token Token) (State, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	cur := w.states[seriesKey]
	if token <= cur.Token {
		return cur, ErrLeaseStale
	}
	if cur.Owner != "" && cur.Owner != holder && w.now().UnixMilli() < cur.ExpiresAt {
		return cur, ErrLeaseConflict
	}
	issued := w.now().UnixMilli()
	ns := State{Owner: holder, Token: token, Version: version, ExpiresAt: issued + ttlMS, IssuedAt: issued}
	w.states[seriesKey] = ns
	return ns, nil
}

// IsConflict reports whether err is a lease conflict (a live competing owner).
func IsConflict(err error) bool {
	var typed *apperror.Error
	if errors.As(err, &typed) {
		return typed.Code == apperror.CodeLeaseConflict
	}
	return false
}
