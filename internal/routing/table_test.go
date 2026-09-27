package routing

import (
	"flag"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/model"
)

func testNodes() []Node {
	return []Node{{ID: "node-a", Weight: 1}, {ID: "node-b", Weight: 1}}
}

func series(suffix string) model.SeriesKey {
	return model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "BBG004730N88" + suffix, Timeframe: "1m", CandleType: "trade"}
}

// TestDeterministicAcrossNodes proves the core P2.1 property: every node
// holding the same membership and routing version computes the identical
// primary/standby assignment. Independent Table instances simulate
// independent processes; input order of members must not matter.
func TestDeterministicAcrossNodes(t *testing.T) {
	tableA, err := NewTable(7, testNodes())
	if err != nil {
		t.Fatal(err)
	}
	tableB, err := NewTable(7, reorder(testNodes()))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range goldenKeys(200) {
		a, err := tableA.Assign(key)
		if err != nil {
			t.Fatal(err)
		}
		b, err := tableB.Assign(key)
		if err != nil {
			t.Fatal(err)
		}
		if a != b {
			t.Fatalf("assignment diverged for %s: %+v vs %+v", key.Identity(), a, b)
		}
	}
}

var goldenUpdate = flag.Bool("golden-update", false, "print the current golden vector block instead of comparing")

// TestGoldenVectors pins the exact primary/standby pairs for a fixed key set
// and routing version. The format is "identity primary standby" per line;
// every node and CI must reproduce these values byte-for-byte. Any
// intentional change to the hash is a routing change: it must bump the
// routing version and regenerate this vector set (-golden-update).
func TestGoldenVectors(t *testing.T) {
	table, err := NewTable(1, testNodes())
	if err != nil {
		t.Fatal(err)
	}
	lines := make([]string, 0, 16)
	for i := range 16 {
		key := series(fmt.Sprintf("-%03d", i))
		assignment, err := table.Assign(key)
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, fmt.Sprintf("%s %s %s", key.Identity(), assignment.Primary, assignment.Standby))
	}
	got := strings.Join(lines, "\n")
	if *goldenUpdate {
		t.Log(got)
		return
	}
	want := strings.TrimSpace(goldenV1)
	if got != want {
		t.Fatalf("golden vectors changed:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// goldenV1 is the pinned vector set for routing_version=1, membership
// {node-a:1, node-b:1}. Regenerate only together with a routing version bump.
const goldenV1 = `
tbank|shares|BBG004730N88-000|1m|trade node-a node-b
tbank|shares|BBG004730N88-001|1m|trade node-a node-b
tbank|shares|BBG004730N88-002|1m|trade node-a node-b
tbank|shares|BBG004730N88-003|1m|trade node-a node-b
tbank|shares|BBG004730N88-004|1m|trade node-b node-a
tbank|shares|BBG004730N88-005|1m|trade node-a node-b
tbank|shares|BBG004730N88-006|1m|trade node-a node-b
tbank|shares|BBG004730N88-007|1m|trade node-b node-a
tbank|shares|BBG004730N88-008|1m|trade node-a node-b
tbank|shares|BBG004730N88-009|1m|trade node-a node-b
tbank|shares|BBG004730N88-010|1m|trade node-a node-b
tbank|shares|BBG004730N88-011|1m|trade node-a node-b
tbank|shares|BBG004730N88-012|1m|trade node-b node-a
tbank|shares|BBG004730N88-013|1m|trade node-a node-b
tbank|shares|BBG004730N88-014|1m|trade node-a node-b
tbank|shares|BBG004730N88-015|1m|trade node-b node-a
`

// TestRoutingVersionChangesOwnership proves the version participates in the
// hash: the manual switch procedure reshuffles assignments deliberately.
func TestRoutingVersionChangesOwnership(t *testing.T) {
	v1, _ := NewTable(1, testNodes())
	v2, _ := NewTable(2, testNodes())
	flipped := 0
	for _, key := range goldenKeys(64) {
		a, _ := v1.Assign(key)
		b, _ := v2.Assign(key)
		if a.Primary != b.Primary {
			flipped++
		}
	}
	if flipped == 0 {
		t.Fatal("routing version bump did not change any ownership")
	}
}

// TestMembershipChangeMovesMinimalOwnership checks the rendezvous property:
// removing one node moves only that node's series to the survivor, and no
// series owned by the survivor moves.
func TestMembershipChangeMovesMinimalOwnership(t *testing.T) {
	base, err := NewTable(1, testNodes())
	if err != nil {
		t.Fatal(err)
	}
	shrunk, err := NewTable(1, []Node{{ID: "node-b", Weight: 1}})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range goldenKeys(128) {
		before, _ := base.Assign(key)
		after, _ := shrunk.Assign(key)
		if before.Primary == "node-a" {
			if after.Primary != "node-b" {
				t.Fatalf("series %s owned by removed node must move to survivor", key.Identity())
			}
			continue
		}
		if before.Primary != after.Primary {
			t.Fatalf("series %s moved between surviving owners: %s -> %s", key.Identity(), before.Primary, after.Primary)
		}
	}
}

// TestWeightedDistribution checks ownership proportions track weights within
// a tolerance for a large fixed key set.
func TestWeightedDistribution(t *testing.T) {
	table, err := NewTable(1, []Node{{ID: "node-a", Weight: 3}, {ID: "node-b", Weight: 1}})
	if err != nil {
		t.Fatal(err)
	}
	const total = 20000
	identities := make([]string, total)
	for i := range identities {
		identities[i] = series(fmt.Sprintf("-%06d", i)).Identity()
	}
	counts := table.Distribution(identities)
	nodeAShare := float64(counts["node-a"]) / total
	if math.Abs(nodeAShare-0.75) > 0.03 {
		t.Fatalf("node-a share=%.4f, want ~0.75", nodeAShare)
	}
}

func TestRejectsInvalidMembership(t *testing.T) {
	if _, err := NewTable(0, testNodes()); err == nil {
		t.Fatal("expected version error")
	}
	if _, err := NewTable(1, nil); err == nil {
		t.Fatal("expected empty membership error")
	}
	if _, err := NewTable(1, []Node{{ID: "a", Weight: 0}}); err == nil {
		t.Fatal("expected weight error")
	}
	if _, err := NewTable(1, []Node{{ID: "", Weight: 1}}); err == nil {
		t.Fatal("expected id error")
	}
	if _, err := NewTable(1, []Node{{ID: "a", Weight: 1}, {ID: "a", Weight: 2}}); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestSingleMemberStandbyEmpty(t *testing.T) {
	table, err := NewTable(1, []Node{{ID: "solo", Weight: 1}})
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := table.Assign(series("-1"))
	if err != nil {
		t.Fatal(err)
	}
	if assignment.Primary != "solo" || assignment.Standby != "" {
		t.Fatalf("assignment=%+v", assignment)
	}
}

func TestAssignRejectsInvalidKey(t *testing.T) {
	table, _ := NewTable(1, testNodes())
	if _, err := table.Assign(model.SeriesKey{}); err == nil {
		t.Fatal("expected validation error")
	}
}

func goldenKeys(n int) []model.SeriesKey {
	keys := make([]model.SeriesKey, 0, n)
	for i := range n {
		keys = append(keys, series(fmt.Sprintf("-%06d", i)))
	}
	return keys
}

func reorder(nodes []Node) []Node {
	out := make([]Node, len(nodes))
	for i, node := range nodes {
		out[len(nodes)-1-i] = node
	}
	return out
}
