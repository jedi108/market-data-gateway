package lease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
)

// OwnerFunc resolves the deterministic primary and standby owner of a series
// from the canonical routing table. Passing ownership resolution in keeps the
// lease package decoupled from routing/model specifics.
type OwnerFunc func(key string) (primary string, standby string, err error)

// Coordinator drives lease acquisition and renewal. It is the only component
// permitted to change ownership in automatic mode; the Gate consults the
// resulting held lease before any provider call.
type Coordinator struct {
	nodeID       string
	version      int
	ttlMS        int64
	store        *NodeStore
	members      []Member // local + currently-reachable witnesses (attempt set)
	totalMembers int      // full configured membership (quorum basis)
	ownerFunc    OwnerFunc
	automatic    bool
	now          func() time.Time
}

// Options configure a Coordinator.
type Options struct {
	NodeID  string
	Version int
	TTLMS   int64
	Store   *NodeStore
	Members []Member // local + currently-reachable witnesses (attempt set)
	// TotalMembers is the full configured membership (local + all witnesses) used
	// as the quorum basis. Quorum is a strict majority of TotalMembers, never of
	// the merely-reachable subset, so a minority partition can never confirm a
	// lease. Defaults to len(Members) when zero.
	TotalMembers int
	OwnerFunc    OwnerFunc
	Automatic    bool
	Now          func() time.Time
}

// NewCoordinator builds a coordinator. members must be non-empty and
// members[0] is the local node's self-vote. OwnerFunc is required.
func NewCoordinator(o Options) (*Coordinator, error) {
	if o.NodeID == "" || o.Store == nil || len(o.Members) == 0 || o.OwnerFunc == nil {
		return nil, fmt.Errorf("coordinator requires node id, store, members and an owner function")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	total := o.TotalMembers
	if total <= 0 {
		total = len(o.Members)
	}
	return &Coordinator{
		nodeID:       o.NodeID,
		version:      o.Version,
		ttlMS:        o.TTLMS,
		store:        o.Store,
		members:      o.Members,
		totalMembers: total,
		ownerFunc:    o.OwnerFunc,
		automatic:    o.Automatic,
		now:          o.Now,
	}, nil
}

// quorumMajority returns the strict-majority count required to win a commit.
// It is computed over the full configured membership, so a minority partition
// (fewer reachable members than the majority of total) can never confirm.
func (c *Coordinator) quorumMajority() int { return c.totalMembers/2 + 1 }

// TTLMillis returns the configured lease TTL in milliseconds. Exposed so the
// composition root can size the renewal cadence.
func (c *Coordinator) TTLMillis() int64 { return c.ttlMS }

// Takeover attempts to acquire the lease for key. In manual mode only the
// deterministic primary may hold it (no quorum, no takeover by standby). In
// automatic mode the candidate must (a) be the deterministic primary or standby,
// (b) observe no unexpired conflicting owner, and (c) collect a strict-majority
// quorum Acquire with a token greater than the observed max. On success the
// lease is marked held locally.
func (c *Coordinator) Takeover(ctx context.Context, key string) (Lease, error) {
	primary, standby, err := c.ownerFunc(key)
	if err != nil {
		return Lease{}, err
	}

	if !c.automatic {
		// Manual failover: ownership is exactly the deterministic primary. A
		// non-primary node must route to the owner and may never call the
		// provider. There is no lease to take over.
		if c.nodeID != primary {
			return Lease{}, ErrLeaseManualMode
		}
		l := Lease{Holder: primary, Token: 1, Version: c.version, ExpiresAt: c.now().UnixMilli() + c.ttlMS, IssuedAt: c.now().UnixMilli()}
		c.store.setHeld(key, l)
		return l, nil
	}

	// Automatic mode: only the deterministic primary or standby may own.
	if c.nodeID != primary && c.nodeID != standby {
		return Lease{}, ErrLeaseManualMode
	}

	// Observe the highest committed token across all members (forward fencing).
	observed := c.observeMax(ctx, key)
	token := observed.Token + 1

	// Acquire from a strict majority. Acquire is atomic and enforces fencing on
	// each member; a member that already holds a higher or live token rejects
	// the commit, so a lost race yields no lease.
	won := 0
	for _, m := range c.members {
		if ctx.Err() != nil {
			return Lease{}, apperror.New(apperror.CodeLeaseQuorumFailed, "takeover canceled", 0)
		}
		if _, err := m.Acquire(key, c.nodeID, c.version, c.ttlMS, token); err != nil {
			if IsConflict(err) || errors.Is(err, ErrLeaseStale) {
				// A newer/live token surfaced during the race: abort safely.
				return Lease{}, err
			}
			continue // unreachable member does not count toward the quorum
		}
		won++
	}
	if won < c.quorumMajority() {
		return Lease{}, ErrLeaseQuorumFailed
	}
	l := Lease{Holder: c.nodeID, Token: token, Version: c.version, ExpiresAt: c.now().UnixMilli() + c.ttlMS, IssuedAt: c.now().UnixMilli()}
	c.store.setHeld(key, l)
	c.store.setObserved(key, State{Owner: c.nodeID, Token: token, Version: c.version, ExpiresAt: l.ExpiresAt, IssuedAt: l.IssuedAt})
	return l, nil
}

// Renew re-confirms the locally held lease by re-acquiring the same token with a
// fresh expiry via a strict majority. It fails closed (no held lease) if the
// quorum cannot be reached, so an isolated node stops calling the provider.
func (c *Coordinator) Renew(ctx context.Context, key string) (Lease, error) {
	held, ok := c.store.Held(key)
	if !ok {
		return Lease{}, ErrLeaseExpired
	}
	won := 0
	for _, m := range c.members {
		if ctx.Err() != nil {
			c.store.dropHeld(key)
			return Lease{}, apperror.New(apperror.CodeLeaseQuorumFailed, "renew canceled", 0)
		}
		// Re-acquire the SAME token with a fresh expiry. Fencing is satisfied
		// because token == the committed token on a healthy member; a higher
		// token elsewhere means we were fenced and the Acquire is rejected.
		if _, err := m.Acquire(key, held.Holder, held.Version, c.ttlMS, held.Token); err != nil {
			if IsConflict(err) {
				c.store.dropHeld(key)
				return Lease{}, ErrLeaseConflict
			}
			continue
		}
		won++
	}
	if won < c.quorumMajority() {
		c.store.dropHeld(key)
		return Lease{}, ErrLeaseQuorumFailed
	}
	renewed := Lease{Holder: held.Holder, Token: held.Token, Version: held.Version, ExpiresAt: c.now().UnixMilli() + c.ttlMS, IssuedAt: held.IssuedAt}
	c.store.setHeld(key, renewed)
	return renewed, nil
}

// observeMax returns the highest committed token any member currently reports
// for key, so the candidate proposes strictly above it (forward fencing).
func (c *Coordinator) observeMax(ctx context.Context, key string) State {
	var max State
	for _, m := range c.members {
		if ctx.Err() != nil {
			break
		}
		if st := m.Peek(key); st.Token > max.Token {
			max = st
		}
	}
	if local := c.store.Observed(key); local.Token > max.Token {
		max = local
	}
	return max
}

// Sync learns the committed token for key from a single reachable member (used
// by a node that lost its held lease to discover whether it was fenced).
func (c *Coordinator) Sync(key string) State {
	best := c.store.Observed(key)
	for _, m := range c.members {
		if st := m.Peek(key); st.Token > best.Token {
			best = st
		}
	}
	c.store.setObserved(key, best)
	return best
}
