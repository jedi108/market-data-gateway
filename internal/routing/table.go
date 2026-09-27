// Package routing implements deterministic series ownership for the cluster.
//
// The owner of a SeriesKey is computed by weighted rendezvous (highest-random-
// weight) hashing over the canonical SeriesKey identity and the routing
// version. Every node holding the same membership and routing version derives
// the identical primary owner and standby without any communication; health
// observations never influence the result. Standby is informational only: it
// never becomes the upstream owner without a new routing version (manual
// switch procedure), which is what prevents split-brain ownership.
package routing

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/jedi108/market-data-gateway/internal/model"
)

// Node is one eligible membership entry. Weight is the relative upstream
// capacity share of the node; it must be positive and is fixed per routing
// version.
type Node struct {
	ID     string
	Weight float64
}

// Table is immutable after construction and safe for concurrent use.
type Table struct {
	nodes          []Node
	routingVersion int
	// sortedIDs is the canonical member order used for deterministic
	// iteration in snapshot/golden output.
	sortedIDs []string
	byID      map[string]Node
}

// NewTable validates membership and freezes the eligible set. A table with
// fewer than one node is invalid; duplicate ids and nonpositive weights are
// configuration errors.
func NewTable(routingVersion int, nodes []Node) (*Table, error) {
	if routingVersion <= 0 {
		return nil, fmt.Errorf("routing version must be positive")
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("membership must contain at least one node")
	}
	byID := make(map[string]Node, len(nodes))
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node.ID == "" {
			return nil, fmt.Errorf("node id is required")
		}
		if math.IsNaN(node.Weight) || node.Weight <= 0 || math.IsInf(node.Weight, 0) {
			return nil, fmt.Errorf("node %q has invalid weight", node.ID)
		}
		if _, exists := byID[node.ID]; exists {
			return nil, fmt.Errorf("duplicate node id %q", node.ID)
		}
		byID[node.ID] = node
		ids = append(ids, node.ID)
	}
	sort.Strings(ids)
	frozen := make([]Node, len(nodes))
	copy(frozen, nodes)
	return &Table{nodes: frozen, routingVersion: routingVersion, sortedIDs: ids, byID: byID}, nil
}

// RoutingVersion returns the frozen routing version of the table.
func (t *Table) RoutingVersion() int { return t.routingVersion }

// NodeIDs returns the canonical sorted membership.
func (t *Table) NodeIDs() []string {
	out := make([]string, len(t.sortedIDs))
	copy(out, t.sortedIDs)
	return out
}

// Node returns the membership entry for id.
func (t *Table) Node(id string) (Node, bool) {
	node, ok := t.byID[id]
	return node, ok
}

// Len returns the number of eligible nodes.
func (t *Table) Len() int { return len(t.nodes) }

// Assignment is the deterministic ownership decision for one SeriesKey.
type Assignment struct {
	Primary string
	Standby string // empty when membership has a single node
}

// Assign computes primary and standby owners for a SeriesKey. All nodes with
// the same table derive the same assignment.
func (t *Table) Assign(key model.SeriesKey) (Assignment, error) {
	if err := key.Validate(); err != nil {
		return Assignment{}, err
	}
	return t.assignIdentity(key.Identity()), nil
}

// assignIdentity is the raw hash core. Callers inside this package have
// already validated the key.
func (t *Table) assignIdentity(identity string) Assignment {
	var primary, standby string
	var primaryScore, standbyScore float64
	for _, node := range t.nodes {
		score := t.score(identity, node)
		if primary == "" || score > primaryScore {
			standby, standbyScore = primary, primaryScore
			primary, primaryScore = node.ID, score
		} else if standby == "" || score > standbyScore {
			standby, standbyScore = node.ID, score
		}
	}
	return Assignment{Primary: primary, Standby: standby}
}

// score is the weighted rendezvous hash: -weight / ln(u) where u is a uniform
// hash in (0,1). The logarithm keeps scores positive and monotone in u for a
// fixed weight, and the weight multiplies the effective capacity share.
func (t *Table) score(identity string, node Node) float64 {
	u := t.uniform(identity, node.ID)
	// u is guaranteed in (0,1) by construction; guard anyway so a degenerate
	// hash can never produce NaN/Inf scores.
	if u <= 0 {
		u = math.SmallestNonzeroFloat64
	}
	if u >= 1 {
		u = 1 - math.SmallestNonzeroFloat64
	}
	return node.Weight / -math.Log(u)
}

// uniform derives a deterministic uniform double in (0,1) from the series
// identity, node id, and routing version. The routing version is part of the
// salt so a new version reshuffles ownership deliberately.
func (t *Table) uniform(identity, nodeID string) float64 {
	h := sha256.New()
	h.Write([]byte("mdg-rdz-v1\n"))
	fmt.Fprintf(h, "rv=%d\n", t.routingVersion)
	fmt.Fprintf(h, "series=%s\n", identity)
	fmt.Fprintf(h, "node=%s\n", nodeID)
	sum := h.Sum(nil)
	// 53 explicit bits keeps the mapping injective onto the float64 mantissa.
	v := binary.BigEndian.Uint64(sum[:8]) >> (64 - 53)
	return float64(v) / (1 << 53)
}

// Ownerships maps every identity in identities to its assignment. It is the
// batch form used for golden-vector verification and distribution checks.
func (t *Table) Ownerships(identities []string) map[string]Assignment {
	out := make(map[string]Assignment, len(identities))
	for _, identity := range identities {
		out[identity] = t.assignIdentity(identity)
	}
	return out
}

// Distribution counts primary ownership over the given identities, keyed by
// node id. Missing nodes report zero.
func (t *Table) Distribution(identities []string) map[string]int {
	counts := make(map[string]int, len(t.nodes))
	for _, id := range t.sortedIDs {
		counts[id] = 0
	}
	for _, identity := range identities {
		counts[t.assignIdentity(identity).Primary]++
	}
	return counts
}
