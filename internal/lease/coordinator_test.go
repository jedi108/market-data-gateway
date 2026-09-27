package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
)

// failClosedMembers returns 3 participants (self + 2 witnesses) that all refuse.
// They model a fully degraded lease store: every acquire fails fast. The gate
// must never let a node call the provider in that state.
func failClosedMembers(nodeID string, now func() time.Time) ([]Member, *NodeStore) {
	store := NewNodeStore(nodeID, now)
	refuse := &refuseMember{}
	return []Member{store, refuse, refuse}, store
}

type refuseMember struct{}

func (refuseMember) Peek(string) State { return State{} }
func (refuseMember) Acquire(string, string, int, int64, Token) (State, error) {
	return State{}, ErrLeaseUnavailable
}

func ownerFn(primary, standby string) OwnerFunc {
	return func(string) (string, string, error) { return primary, standby, nil }
}

// --- Gate 6 proof: at most one upstream-owner per SeriesKey ----------------

// TestManualModeStaleOwnerRejected proves that in manual mode a non-primary node
// (an "old owner" that lost the deterministic primary role) may never call the
// provider: Takeover is refused and the gate denies the provider call.
func TestManualModeStaleOwnerRejected(t *testing.T) {
	now := time.Now
	members, store := failClosedMembers("node-a", now)
	c, err := NewCoordinator(Options{NodeID: "node-a", Version: 1, TTLMS: 30_000, Store: store, Members: members, OwnerFunc: ownerFn("node-b", "node-a"), Automatic: false, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	// node-a is the standby in manual mode -> not allowed to own.
	if _, err := c.Takeover(context.Background(), "tbank|shares|BBG|1m|trade"); err != ErrLeaseManualMode {
		t.Fatalf("standby takeover in manual mode should be refused, got %v", err)
	}
	g, _ := NewGate(GateOptions{Store: store, Automatic: false, OwnerFunc: ownerFn("node-b", "node-a")})
	if d := g.MayCallProvider("tbank|shares|BBG|1m|trade"); d.Allowed {
		t.Fatalf("manual-mode standby must not be allowed to call provider: %+v", d)
	}
}

// TestAutomaticMinorityPartitionFailsClosed proves the central split-brain
// guarantee: with membership = {primary, standby, 2 witnesses} (5 → majority 3),
// if the standby is isolated from the witnesses (sees only 1 of 5 = itself) it
// cannot reach quorum, so it may NOT take over and may NOT call the provider. A
// minority partition can never create a second owner.
func TestAutomaticMinorityPartitionFailsClosed(t *testing.T) {
	now := time.Now
	key := "tbank|shares|BBG|1m|trade"

	// Standby's view: only itself as a member (witnesses unreachable / partitioned),
	// but the quorum basis is the full configured membership of 5 (self+primary+2
	// witnesses). Majority of 5 is 3, so a single reachable member can never win.
	standbyStore := NewNodeStore("node-a", now)
	standbyMembers := []Member{standbyStore}
	c, err := NewCoordinator(Options{NodeID: "node-a", Version: 1, TTLMS: 30_000, Store: standbyStore, Members: standbyMembers, TotalMembers: 5, OwnerFunc: ownerFn("node-b", "node-a"), Automatic: true, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Takeover(context.Background(), key)
	if !errors.Is(err, ErrLeaseQuorumFailed) {
		t.Fatalf("isolated standby must fail quorum, got %v", err)
	}
	g, _ := NewGate(GateOptions{Store: standbyStore, Automatic: true, OwnerFunc: ownerFn("node-b", "node-a")})
	if d := g.MayCallProvider(key); d.Allowed {
		t.Fatalf("isolated standby must be denied provider call: %+v", d)
	}

	// Primary's view: full membership; it can confirm itself (token 1) and keep
	// the lease valid. Two nodes still form no majority of 5, but the PRIMARY
	// already won and holds token 1 from before the partition; the gate must
	// keep allowing it (it is the healthy majority side's deterministic owner).
	// Here we simply assert the primary can re-confirm via a 3-member majority
	// that remains reachable (primary + 2 witnesses on its side).
	primaryStore := NewNodeStore("node-b", now)
	w1, w2 := NewWitnessStore(now), NewWitnessStore(now)
	primaryMembers := []Member{primaryStore, w1, w2}
	pc, err := NewCoordinator(Options{NodeID: "node-b", Version: 1, TTLMS: 30_000, Store: primaryStore, Members: primaryMembers, OwnerFunc: ownerFn("node-b", "node-a"), Automatic: true, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	l, err := pc.Takeover(context.Background(), key)
	if err != nil {
		t.Fatalf("primary side with majority must win lease: %v", err)
	}
	if l.Holder != "node-b" || l.Token != 1 {
		t.Fatalf("unexpected primary lease %+v", l)
	}
	pg, _ := NewGate(GateOptions{Store: primaryStore, Automatic: true, OwnerFunc: ownerFn("node-b", "node-a")})
	if d := pg.MayCallProvider(key); !d.Allowed {
		t.Fatalf("primary side majority must be allowed: %+v", d)
	}
	// The committed token is now 1 on the witnesses; the isolated standby's
	// observed token is 0, so even if it later held a forged lease the gate's
	// fencing check (held < observed) would deny it. Simulate: standby learns
	// observed=1 from a synced witness.
	standbyStore.setObserved(key, State{Owner: "node-b", Token: 1, ExpiresAt: now().UnixMilli() + 30_000})
	if d := g.MayCallProvider(key); d.Allowed {
		t.Fatalf("standby fenced by newer token must be denied: %+v", d)
	}
}

// TestQuorumTakeoverSingleWinner proves that when exactly one node wins the
// quorum, the loser cannot also acquire (fencing), so there is never more than
// one owner simultaneously.
func TestQuorumTakeoverSingleWinner(t *testing.T) {
	now := time.Now
	key := "tbank|shares|BBG|1h|trade"

	// Shared witnesses act as the real source of truth for both candidates.
	w1, w2 := NewWitnessStore(now), NewWitnessStore(now)

	standbyStore := NewNodeStore("node-a", now)
	standby := []Member{standbyStore, w1, w2}
	sc, _ := NewCoordinator(Options{NodeID: "node-a", Version: 1, TTLMS: 30_000, Store: standbyStore, Members: standby, OwnerFunc: ownerFn("node-b", "node-a"), Automatic: true, Now: now})

	primaryStore := NewNodeStore("node-b", now)
	primary := []Member{primaryStore, w1, w2}
	pc, _ := NewCoordinator(Options{NodeID: "node-b", Version: 1, TTLMS: 30_000, Store: primaryStore, Members: primary, OwnerFunc: ownerFn("node-b", "node-a"), Automatic: true, Now: now})

	// Primary wins first (token 1 committed on witnesses).
	if _, err := pc.Takeover(context.Background(), key); err != nil {
		t.Fatalf("primary takeover: %v", err)
	}
	// Standby attempt must fail: witness already holds token 1 unexpired.
	_, err := sc.Takeover(context.Background(), key)
	if err == nil {
		t.Fatal("standby must be denied: primary already owns via quorum")
	}
	if !IsConflict(err) && !errors.Is(err, ErrLeaseQuorumFailed) && !errors.Is(err, ErrLeaseStale) {
		t.Fatalf("standby denial reason unexpected: %v", err)
	}
	// Gate: only primary allowed; standby denied.
	pg, _ := NewGate(GateOptions{Store: primaryStore, Automatic: true, OwnerFunc: ownerFn("node-b", "node-a")})
	sg, _ := NewGate(GateOptions{Store: standbyStore, Automatic: true, OwnerFunc: ownerFn("node-b", "node-a")})
	if d := pg.MayCallProvider(key); !d.Allowed {
		t.Fatalf("primary should be allowed: %+v", d)
	}
	if d := sg.MayCallProvider(key); d.Allowed {
		t.Fatalf("standby must be denied while primary owns: %+v", d)
	}
}

// TestLeaseExpiryStopsProvider proves that once a held lease expires (primary
// crash / renewal failure) the gate stops the provider call, forcing the
// stale/replica fallback path instead of a second owner emerging spontaneously.
func TestLeaseExpiryStopsProvider(t *testing.T) {
	now := time.Now
	key := "tbank|shares|GAZ|5m|trade"
	store := NewNodeStore("node-b", now)
	// Held lease already expired.
	store.setHeld(key, Lease{Holder: "node-b", Token: 1, Version: 1, ExpiresAt: now().UnixMilli() - 1, IssuedAt: now().UnixMilli() - 60_000})
	g, _ := NewGate(GateOptions{Store: store, Automatic: true, OwnerFunc: ownerFn("node-b", "node-a")})
	if d := g.MayCallProvider(key); d.Allowed {
		t.Fatalf("expired lease must deny provider call: %+v", d)
	}
}

// TestRenewalDropsLeaseOnQuorumLoss proves the renewal path fails closed: if a
// previously-held lease can no longer be re-confirmed by a majority, the node
// drops the lease and the gate denies the provider call.
func TestRenewalDropsLeaseOnQuorumLoss(t *testing.T) {
	now := time.Now
	key := "tbank|shares|GAZ|1m|trade"
	store := NewNodeStore("node-b", now)
	store.setHeld(key, Lease{Holder: "node-b", Token: 1, Version: 1, ExpiresAt: now().UnixMilli() + 30_000, IssuedAt: now().UnixMilli()})
	// Witnesses become unreachable mid-life.
	refuse := &refuseMember{}
	c, _ := NewCoordinator(Options{NodeID: "node-b", Version: 1, TTLMS: 30_000, Store: store, Members: []Member{store, refuse, refuse}, OwnerFunc: ownerFn("node-b", "node-a"), Automatic: true, Now: now})
	if _, err := c.Renew(context.Background(), key); err != ErrLeaseQuorumFailed {
		t.Fatalf("renew must fail quorum, got %v", err)
	}
	if _, ok := store.Held(key); ok {
		t.Fatal("renew failure must drop the held lease")
	}
	g, _ := NewGate(GateOptions{Store: store, Automatic: true, OwnerFunc: ownerFn("node-b", "node-a")})
	if d := g.MayCallProvider(key); d.Allowed {
		t.Fatalf("post-renew-failure node must be denied: %+v", d)
	}
}

// TestDeterministicRoutingPreserved verifies the manual (default) path still
// produces exactly one owner per SeriesKey from the routing table and that the
// lease control plane never changes it without a routing_version bump. This is
// the rollback guarantee: manual switch remains the safe fallback.
func TestDeterministicRoutingPreserved(t *testing.T) {
	now := time.Now
	key := "tbank|shares|BBG|1m|trade"
	// Same owner function on both nodes => identical deterministic ownership.
	of := ownerFn("node-b", "node-a")
	if p1, s1, _ := of(key); p1 != "node-b" || s1 != "node-a" {
		t.Fatalf("owner function mismatch: %s/%s", p1, s1)
	}
	// Primary wins an implicit manual lease; standby refused; routing version
	// unchanged means no takeover. This is the documented rollback state.
	primaryStore := NewNodeStore("node-b", now)
	pc, _ := NewCoordinator(Options{NodeID: "node-b", Version: 1, TTLMS: 30_000, Store: primaryStore, Members: []Member{primaryStore}, OwnerFunc: of, Automatic: false, Now: now})
	if _, err := pc.Takeover(context.Background(), key); err != nil {
		t.Fatalf("primary manual takeover: %v", err)
	}
	standbyStore := NewNodeStore("node-a", now)
	sc, _ := NewCoordinator(Options{NodeID: "node-a", Version: 1, TTLMS: 30_000, Store: standbyStore, Members: []Member{standbyStore}, OwnerFunc: of, Automatic: false, Now: now})
	if _, err := sc.Takeover(context.Background(), key); err != ErrLeaseManualMode {
		t.Fatalf("standby manual takeover must be refused, got %v", err)
	}
}

// TestFencingTokenMonotonic proves tokens only move forward and a stale token is
// rejected by a witness, the atomic unit of split-brain prevention.
func TestFencingTokenMonotonic(t *testing.T) {
	now := time.Now
	w := NewWitnessStore(now)
	key := "tbank|shares|SBER|1d|trade"
	if _, err := w.Acquire(key, "node-b", 1, 30_000, 1); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	// Same token (replay) rejected.
	if _, err := w.Acquire(key, "node-b", 1, 30_000, 1); !errors.Is(err, ErrLeaseStale) {
		t.Fatalf("replay token must be stale, got %v", err)
	}
	// Lower token rejected.
	if _, err := w.Acquire(key, "node-a", 1, 30_000, 0); !errors.Is(err, ErrLeaseStale) {
		t.Fatalf("lower token must be stale, got %v", err)
	}
	// A different holder with a HIGHER token is STILL rejected while the current
	// lease is unexpired: a healthy owner is never silently fenced by a newcomer.
	// This is the core split-brain guard.
	if _, err := w.Acquire(key, "node-a", 1, 30_000, 2); !IsConflict(err) {
		t.Fatalf("higher token from another holder must conflict while unexpired, got %v", err)
	}
	// After expiry (and only after), a higher token from a new holder wins.
	expired := NewWitnessStore(func() time.Time { return now().Add(60 * time.Second) })
	// Seed expired store with the original lease (expired now).
	expired.states[key] = State{Owner: "node-b", Token: 1, ExpiresAt: now().UnixMilli() + 30_000}
	if moved, err := expired.Acquire(key, "node-a", 1, 30_000, 2); err != nil || moved.Token != 2 || moved.Owner != "node-a" {
		t.Fatalf("after expiry, higher token must win: %+v %v", moved, err)
	}
}

// TestGateErrorIsTyped verifies the gate denial maps to a typed lease error the
// data plane can branch on without string matching.
func TestGateErrorIsTyped(t *testing.T) {
	now := time.Now
	key := "tbank|shares|SBER|1m|trade"
	store := NewNodeStore("node-a", now)
	g, _ := NewGate(GateOptions{Store: store, Automatic: false, OwnerFunc: ownerFn("node-b", "node-a")})
	d := g.MayCallProvider(key)
	if d.Allowed {
		t.Fatal("expected denial")
	}
	var typed *apperror.Error
	if !errors.As(d.DeniedAsError(), &typed) {
		t.Fatalf("denial must be a typed apperror, got %v", d.DeniedAsError())
	}
}
