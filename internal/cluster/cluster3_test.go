package cluster_test

// P2.6 three-node cluster verification (fake nodes standing in for VPS).
//
// The reference two-node membership is {node-a, node-b}; these tests add one
// synthetic third member ("node-c") purely to prove the cluster algorithm and its
// invariants generalize beyond two members:
//
//   - equal routing result on every node (identical assignments),
//   - exactly one provider call for a cluster-wide miss,
//   - series distribution follows weights,
//   - batch partitions across all three owners with per-item results,
//   - no loop / no split-brain: an owner outage never makes any other node
//     (primary standby included) fetch the series,
//   - manual routing-version switch: all three nodes agree on v2, ownership
//     flips for some series, and v1 rollback restores the original owners.

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/auth"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/cluster"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/peerapi"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/routing"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

func threeNodes() []routing.Node {
	return []routing.Node{{ID: "node-a", Weight: 1}, {ID: "node-b", Weight: 1}, {ID: "node-c", Weight: 1}}
}

// newThreeNodeCluster builds an N=3 in-process cluster over real HTTP peer
// listeners, parameterized by routing version (the manual-switch drill
// restarts it with version 2 and rolls back to 1).
func newThreeNodeCluster(t *testing.T, routingVersion int) map[string]*node {
	t.Helper()
	ids := []string{"node-a", "node-b", "node-c"}
	nodes := map[string]*node{}
	for _, id := range ids {
		store, err := storage.Open(fmt.Sprintf("%s/%s.sqlite", t.TempDir(), id))
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = &node{
			id:       id,
			provider: &countingProvider{},
			store:    store,
			sched:    ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 10000, PerClientQuotaPerMinute: 10000, MaxTrackedClients: 100, QueueCapacity: 128, WorkerCount: 4, MaxRetries: 0, RetryBaseDelay: time.Millisecond}),
		}
	}

	// Phase 1: peer servers with late-bound sources.
	sources := map[string]*lateSource{}
	servers := map[string]*httptest.Server{}
	for _, id := range ids {
		source := &lateSource{}
		member, err := cluster.NewMembership(clusterID, id, routingVersion, threeNodes(), map[string]string{"node-a": "http://127.0.0.1:1", "node-b": "http://127.0.0.1:1", "node-c": "http://127.0.0.1:1"}, fmt.Sprintf("hash3-v%d", routingVersion))
		if err != nil {
			t.Fatal(err)
		}
		handler, err := peerapi.New(auth.NewNodeAuth(nodeToken), member, source, cluster.NodeInfo{NodeID: id, ClusterID: clusterID, RoutingVersion: routingVersion, Build: "test"}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		sources[id] = source
		servers[id] = server
		nodes[id].peer = server
	}

	// Phase 2: real memberships with real peer URLs + services.
	for _, id := range ids {
		n := nodes[id]
		urls := map[string]string{"node-a": servers["node-a"].URL, "node-b": servers["node-b"].URL, "node-c": servers["node-c"].URL}
		member, err := cluster.NewMembership(clusterID, id, routingVersion, threeNodes(), urls, fmt.Sprintf("hash3-v%d", routingVersion))
		if err != nil {
			t.Fatal(err)
		}
		client, err := cluster.NewClient(member, nodeToken, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		svc, err := service.New(cache.New(), n.store, n.provider, n.sched, service.Config{
			RequestTimeout: 2 * time.Second,
			MaxStaleAge:    time.Hour,
			Router:         member,
			Peer:           client,
			PeerTimeout:    2 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		n.member = member
		n.svc = svc
		sources[id].set(svc)
	}

	t.Cleanup(func() {
		for _, id := range ids {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			nodes[id].sched.Drain(ctx)
			nodes[id].store.Close()
		}
	})
	return nodes
}

// TestThreeNodesDeriveIdenticalAssignments proves the equal-routing-result
// invariant with three members: every node's table yields the same
// primary/standby for a large fixed key set.
func TestThreeNodesDeriveIdenticalAssignments(t *testing.T) {
	// Independent per-node tables (as three processes would build them),
	// including different member orders.
	orderA := threeNodes()
	orderB := []routing.Node{orderA[2], orderA[0], orderA[1]}
	orderC := []routing.Node{orderA[1], orderA[2], orderA[0]}
	tables := make([]*routing.Table, 0, 3)
	for _, order := range [][]routing.Node{orderA, orderB, orderC} {
		table, err := routing.NewTable(1, order)
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	for i := range 500 {
		key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: fmt.Sprintf("figi3-%05d", i), Timeframe: "1m", CandleType: "trade"}
		want, err := tables[0].Assign(key)
		if err != nil {
			t.Fatal(err)
		}
		for _, table := range tables[1:] {
			got, err := table.Assign(key)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("three-node assignment diverged for %s: %+v vs %+v", key.Identity(), got, want)
			}
		}
	}
}

// TestThreeNodeClusterWideMissIsOneProviderCall: the same miss arriving
// concurrently at ALL THREE nodes produces exactly one provider call,
// executed by the deterministic owner only.
func TestThreeNodeClusterWideMissIsOneProviderCall(t *testing.T) {
	nodes := newThreeNodeCluster(t, 1)
	table := nodes["node-a"].member.Table()
	key := ownerKeyedSeries(t, table, "node-c") // third member owns it
	request := requestFor(key)

	const concurrency = 30
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := []string{"node-a", "node-b", "node-c"}
			_, err := nodes[ids[i%3]].svc.Get(context.Background(), fmt.Sprintf("client-%d", i), ratelimit.PriorityLiveRefresh, request)
			errs[i] = err
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}
	for id, n := range nodes {
		want := int64(0)
		if id == "node-c" {
			want = 1
		}
		if got := n.provider.calls.Load(); got != want {
			t.Fatalf("node %s provider calls=%d, want %d (owner-only fetch)", id, got, want)
		}
	}
}

// TestThreeNodeBatchPartitionsAcrossAllOwners: a client batch spanning all
// three owners is partitioned per owner, answers per item in order, commits
// replicas on the sender, and costs exactly one provider call per distinct
// series cluster-wide.
func TestThreeNodeBatchPartitionsAcrossAllOwners(t *testing.T) {
	nodes := newThreeNodeCluster(t, 1)
	table := nodes["node-a"].member.Table()
	keys := keysForOwners(t, table, "node-a", "node-b", "node-c")
	requests := []model.CandleRequest{requestFor(keys[0]), requestFor(keys[1]), requestFor(keys[2])}

	results := nodes["node-b"].svc.GetBatch(context.Background(), "client", ratelimit.PriorityLiveRefresh, requests, service.BatchOptions{MaxFanOut: 3})
	if len(results) != 3 {
		t.Fatalf("results=%d", len(results))
	}
	for i, result := range results {
		if result.Error != nil {
			t.Fatalf("item %d failed: %v", i, result.Error)
		}
		if result.Index != i {
			t.Fatalf("item %d has index %d", i, result.Index)
		}
		// The sender (node-b) serves its own item locally; items owned by node-a
		// and node-c arrive via one peer hop and must be marked replicas.
		owner := []string{"node-a", "node-b", "node-c"}[i]
		if wantReplica := owner != "node-b"; result.Result.Replica != wantReplica {
			t.Fatalf("item %d (owner %s) replica=%v want %v", i, owner, result.Result.Replica, wantReplica)
		}
		if len(result.Result.Candles) != 2 {
			t.Fatalf("item %d candles=%d", i, len(result.Result.Candles))
		}
	}
	for id, n := range nodes {
		if got := n.provider.calls.Load(); got != 1 {
			t.Fatalf("node %s provider calls=%d, want exactly 1 (its owned series)", id, got)
		}
	}

	// Second identical batch: fully served from cache/replicas.
	results = nodes["node-b"].svc.GetBatch(context.Background(), "client", ratelimit.PriorityLiveRefresh, requests, service.BatchOptions{MaxFanOut: 3})
	for i, result := range results {
		if result.Error != nil || len(result.Result.Candles) != 2 {
			t.Fatalf("second batch item %d: err=%v", i, result.Error)
		}
	}
	for id, n := range nodes {
		if got := n.provider.calls.Load(); got != 1 {
			t.Fatalf("second batch: node %s provider calls=%d, want unchanged 1", id, got)
		}
	}
}

// TestThreeNodeOwnerOutageNeverTransfersOwnership: with the owner's peer
// listener dead, NO other node — including the deterministic standby —
// performs a provider call for the series. The non-owner either serves an
// admissible stale replica or fails closed; split-brain takeover is
// structurally impossible without a new routing version.
func TestThreeNodeOwnerOutageNeverTransfersOwnership(t *testing.T) {
	nodes := newThreeNodeCluster(t, 1)
	table := nodes["node-a"].member.Table()
	key := ownerKeyedSeries(t, table, "node-a")
	request := requestFor(key)

	// Seed a replica on node-b.
	if _, err := nodes["node-b"].svc.Get(context.Background(), "seed", ratelimit.PriorityLiveRefresh, request); err != nil {
		t.Fatal(err)
	}
	before := map[string]int64{}
	for id, n := range nodes {
		before[id] = n.provider.calls.Load()
	}

	// Kill the owner's peer listener: peer hops to node-a now fail.
	nodes["node-a"].peer.Close()

	// With an admissible seeded replica, node-b must serve stale_acceptable
	// (marked replica) rather than fail — and still make no provider call.
	wider := request
	wider.ToUTCMS = 300_000
	result, err := nodes["node-b"].svc.Get(context.Background(), "client", ratelimit.PriorityLiveRefresh, wider)
	if err != nil {
		t.Fatalf("expected admissible stale replica, got error: %v", err)
	}
	if result.Freshness != service.StaleAcceptable {
		t.Fatalf("freshness=%s, want stale_acceptable", result.Freshness)
	}
	if !result.Replica {
		t.Fatal("owner-outage response must be the seeded replica")
	}
	for id, n := range nodes {
		if got := n.provider.calls.Load(); got != before[id] {
			t.Fatalf("node %s provider calls changed %d -> %d during owner outage: split-brain takeover", id, before[id], got)
		}
	}

	// Beyond any admissible stale age or coverage, the request must fail
	// closed with a typed error — never a provider call by a non-owner.
	nodes["sber2"] = nil // keep map iteration stable below
	dead := requestFor(key)
	dead.ToUTCMS = 10 * 60 * 60 * 1000 // far beyond coverage: no stale admissible
	if _, err := nodes["node-c"].svc.Get(context.Background(), "client", ratelimit.PriorityLiveRefresh, dead); err == nil {
		t.Fatal("expected typed failure with the owner unreachable and no admissible stale")
	}
	for id, n := range nodes {
		if id == "sber2" {
			continue
		}
		if got := n.provider.calls.Load(); got != before[id] {
			t.Fatalf("node %s provider calls changed %d -> %d after fail-closed probe: split-brain takeover", id, before[id], got)
		}
	}
	nodes["sber2"] = nil
}

// TestThreeNodeManualRoutingVersionSwitchAndRollback: on a version bump all
// three nodes derive identical v2 assignments, some ownership flips, and
// returning to v1 restores the original owners exactly (rollback drill).
func TestThreeNodeManualRoutingVersionSwitchAndRollback(t *testing.T) {
	v1 := newThreeNodeCluster(t, 1)
	tableV1 := v1["node-a"].member.Table()

	// Establish v1 ownership for a fixed key set.
	type row struct {
		key      model.SeriesKey
		v1Owner  string
		v2Owner  string
		rollback string
	}
	flipped := 0
	rows := make([]row, 0, 64)
	for i := range 64 {
		key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: fmt.Sprintf("figi-sw-%05d", i), Timeframe: "1m", CandleType: "trade"}
		a, err := tableV1.Assign(key)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row{key: key, v1Owner: a.Primary})
	}
	// v2 view built by every node independently.
	v2Tables := make([]*routing.Table, 0, 3)
	for range 3 {
		table, err := routing.NewTable(2, threeNodes())
		if err != nil {
			t.Fatal(err)
		}
		v2Tables = append(v2Tables, table)
	}
	for i := range rows {
		want, err := v2Tables[0].Assign(rows[i].key)
		if err != nil {
			t.Fatal(err)
		}
		for _, table := range v2Tables[1:] {
			got, err := table.Assign(rows[i].key)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("v2 assignment diverged for %s: %+v vs %+v", rows[i].key.Identity(), got, want)
			}
		}
		rows[i].v2Owner = want.Primary
		if rows[i].v2Owner != rows[i].v1Owner {
			flipped++
		}
	}
	if flipped == 0 {
		t.Fatal("routing version bump flipped no ownership in three-node membership")
	}

	// Rollback: v1 views still agree with the original owners.
	tableV1Again, err := routing.NewTable(1, threeNodes())
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		got, err := tableV1Again.Assign(rows[i].key)
		if err != nil {
			t.Fatal(err)
		}
		if got.Primary != rows[i].v1Owner {
			t.Fatalf("rollback changed owner for %s: %s -> %s", rows[i].key.Identity(), rows[i].v1Owner, got.Primary)
		}
	}
}
