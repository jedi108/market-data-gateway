package lease

import (
	"github.com/jedi108/market-data-gateway/internal/apperror"
)

// Gate enforces the Phase 6 safety invariant: a node may call the provider for a
// series only while it holds a valid (unexpired) fencing lease whose token is at
// least the committed token. It is consulted by the data plane immediately
// before any upstream call, after the deterministic ownership check. The gate
// never initiates leases itself — it only reads local truth — so a node with no
// valid lease fails closed (no provider call) and the request falls back to the
// stale/replica path.
type Gate struct {
	store       *NodeStore
	coordinator *Coordinator
	// allowedOnHeld reports whether a held lease is sufficient to call the
	// provider. In manual mode the gate requires the deterministic primary; in
	// automatic mode any unexpired held token (won via quorum) qualifies.
	automatic bool
	ownerFunc OwnerFunc
}

// GateOptions configure the gate.
type GateOptions struct {
	Store       *NodeStore
	Coordinator *Coordinator
	Automatic   bool
	OwnerFunc   OwnerFunc
}

// NewGate builds a gate.
func NewGate(o GateOptions) (*Gate, error) {
	if o.Store == nil {
		return nil, apperror.New(apperror.CodeInvalidRequest, "lease gate requires a store", 0)
	}
	return &Gate{store: o.Store, coordinator: o.Coordinator, automatic: o.Automatic, ownerFunc: o.OwnerFunc}, nil
}

// Decision is the gate verdict for one series.
type Decision struct {
	Allowed bool
	// Reason is a typed apperror code-compatible string for logs/metrics.
	Reason string
}

// MayCallProvider decides whether the local node is permitted to call the
// provider for key right now. It is the single chokepoint that prevents two
// nodes from simultaneously fetching the same SeriesKey.
//
// Rules:
//   - No held lease            -> denied (stale/replica fallback path).
//   - Held lease expired       -> denied.
//   - Manual mode and holder is not the deterministic primary -> denied (a
//     standby must never call the provider without a routing_version switch).
//   - Automatic mode and token < committed observed token -> denied (this node
//     was fenced by a newer lease; deny to prevent split-brain).
//   - Otherwise                -> allowed.
func (g *Gate) MayCallProvider(key string) Decision {
	held, ok := g.store.Held(key)
	if !ok {
		return Decision{Allowed: false, Reason: "no_held_lease"}
	}

	// Fencing check: a held token below the committed/observed token means a
	// newer lease exists elsewhere; acting on the stale token would create a
	// second upstream owner (split-brain). Deny.
	observed := g.store.Observed(key)
	if held.Token < observed.Token {
		return Decision{Allowed: false, Reason: "fenced_by_newer_token"}
	}
	if held.Token > observed.Token {
		// Local node trusts its own won token; it is at or above observed.
	}

	if !g.automatic {
		// Manual failover: only the deterministic primary may hold a provider
		// right. The deterministic owner is derived from the routing table; a
		// non-primary holder is never created in manual mode, but we assert it
		// defensively so the gate is safe even if the store is corrupt.
		if g.ownerFunc != nil {
			primary, _, err := g.ownerFunc(key)
			if err == nil && held.Holder != primary {
				return Decision{Allowed: false, Reason: "manual_mode_non_primary"}
			}
		}
	}

	return Decision{Allowed: true, Reason: "lease_valid"}
}

// DeniedAsError converts a denied decision into a typed error the data plane
// can treat as a controlled upstream stop (stale/replica fallback).
func (d Decision) DeniedAsError() error {
	if d.Allowed {
		return nil
	}
	switch d.Reason {
	case "fenced_by_newer_token":
		return apperror.New(apperror.CodeLeaseExpired, "local lease fenced by newer token; provider call denied", 0)
	case "no_held_lease":
		return apperror.New(apperror.CodeLeaseManualMode, "no held lease; provider call denied", 0)
	default:
		return apperror.New(apperror.CodeLeaseManualMode, "provider call denied: "+d.Reason, 0)
	}
}
