// Package budget guards the cluster-wide upstream rate-limit invariant:
// the sum of all node hard shares for a credential group must never exceed
// the safe user budget of that group. Each node derives the identical
// configured view from the shared cluster config, so every node rejects the
// same unsafe configuration and blocks its own upstream work while the
// invariant is violated — the provider call path is fail-closed, not merely
// warned.
package budget

import (
	"errors"
	"sync/atomic"
)

// ErrUnsafeClusterBudget is returned by Guard.Admit whenever the configured
// share set violates the cluster invariant, or after ObserveShrink has latched
// a smaller observed budget. Callers must treat it as a hard block on every
// upstream provider call for the credential group.
var ErrUnsafeClusterBudget = errors.New("cluster budget invariant violated: node hard shares exceed the safe user budget")

// Share is one node's configured hard upstream budget for a credential group.
type Share struct {
	NodeID              string
	HardBudgetPerMinute int
}

// Snapshot is the exported configured/observed view for metrics and tests.
// SafeBudget is the configured cluster safe budget; ConfiguredSum is the sum
// of every member node's hard share; ObservedBudget is the smallest budget
// the provider has ever reported live (0 = never observed), and it can only
// shrink while the gateway runs.
type Snapshot struct {
	ClusterID       string
	CredentialGroup string
	SafeBudget      int
	ConfiguredSum   int
	LocalNodeID     string
	LocalShare      int
	ObservedBudget  int
	ObservedShrunk  bool
}

// Guard is the per-credential-group cluster budget guard. It is safe for
// concurrent use.
type Guard struct {
	clusterID       string
	credentialGroup string
	safeBudget      int
	localNodeID     string
	localShare      int
	configuredSum   int
	safe            bool

	// observed is the smallest provider-reported budget for the group; a
	// shrink latches the guard closed permanently until process restart with
	// a corrected configuration.
	observed atomic.Int64
	shrunk   atomic.Bool
}

// New validates the configured share set. shares must contain one entry per
// cluster member (this is exactly what the canonical cluster config holds on
// every node). The guard is closed when any share is nonpositive, when the
// local node is missing from the set, or when sum(shares) > safeBudget.
func New(clusterID, credentialGroup string, safeBudget int, localNodeID string, shares []Share) (*Guard, error) {
	if clusterID == "" || credentialGroup == "" || safeBudget <= 0 {
		return nil, ErrUnsafeClusterBudget
	}
	sum := 0
	foundLocal := false
	for _, share := range shares {
		if share.NodeID == "" || share.HardBudgetPerMinute <= 0 {
			return nil, ErrUnsafeClusterBudget
		}
		sum += share.HardBudgetPerMinute
		foundLocal = foundLocal || share.NodeID == localNodeID
	}
	if !foundLocal {
		return nil, ErrUnsafeClusterBudget
	}
	g := &Guard{clusterID: clusterID, credentialGroup: credentialGroup, safeBudget: safeBudget, localNodeID: localNodeID, configuredSum: sum, safe: sum <= safeBudget}
	if !g.safe {
		return nil, ErrUnsafeClusterBudget
	}
	return g, nil
}

// Admit is called before every upstream provider call. It returns
// ErrUnsafeClusterBudget when the configured invariant is violated or a live
// provider observation has shrunk the effective budget below the configured
// sum; in that state upstream work is blocked for the credential group.
func (g *Guard) Admit() error {
	if !g.safe {
		return ErrUnsafeClusterBudget
	}
	if g.shrunk.Load() {
		return ErrUnsafeClusterBudget
	}
	return nil
}

// ObserveShrink records a provider-reported budget (remaining/reset derived
// per-minute capacity). Only shrinkage matters: a larger observed budget than
// configured cannot grant capacity the config never reserved, so it is
// ignored. A shrink below the configured sum latches the guard closed.
func (g *Guard) ObserveShrink(reportedPerMinute int) {
	if reportedPerMinute <= 0 || reportedPerMinute >= g.safeBudget {
		return
	}
	for {
		current := g.observed.Load()
		if current != 0 && current <= int64(reportedPerMinute) {
			break
		}
		if g.observed.CompareAndSwap(current, int64(reportedPerMinute)) {
			break
		}
	}
	if g.observed.Load() < int64(g.configuredSum) {
		g.shrunk.Store(true)
	}
}

// Snapshot returns the configured/observed view for export.
func (g *Guard) Snapshot() Snapshot {
	observed := int(g.observed.Load())
	return Snapshot{
		ClusterID:       g.clusterID,
		CredentialGroup: g.credentialGroup,
		SafeBudget:      g.safeBudget,
		ConfiguredSum:   g.configuredSum,
		LocalNodeID:     g.localNodeID,
		LocalShare:      g.localShare,
		ObservedBudget:  observed,
		ObservedShrunk:  g.shrunk.Load(),
	}
}

// Safe reports the static configured invariant result.
func (g *Guard) Safe() bool { return g.safe && !g.shrunk.Load() }
