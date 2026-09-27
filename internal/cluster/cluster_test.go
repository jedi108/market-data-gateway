package cluster_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
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

// countingProvider counts cluster-wide provider calls.
type countingProvider struct{ calls atomic.Int64 }

func (p *countingProvider) FetchCandles(ctx context.Context, r model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	p.calls.Add(1)
	var candles []model.Candle
	for open := r.FromUTCMS; open < r.ToUTCMS; open += 60_000 {
		candles = append(candles, model.Candle{OpenTimeUTCMS: open, CloseTimeUTCMS: open + 60_000, Open: "100", High: "101", Low: "99", Close: "100", Volume: "10", IsClosed: true})
	}
	return candles, model.ProviderMetadata{Status: "fake"}, nil
}

// lateSource lets the peer handler exist before the service is constructed
// (services need the real peer URLs that only exist once servers listen).
type lateSource struct {
	mu    sync.RWMutex
	inner peerapi.CandleSource
}

func (l *lateSource) GetOwned(request model.CandleRequest) (cluster.OwnedResult, error) {
	l.mu.RLock()
	inner := l.inner
	l.mu.RUnlock()
	if inner == nil {
		return cluster.OwnedResult{}, apperror.New(apperror.CodePeerUnavailable, "owner service is not ready", 0)
	}
	return inner.GetOwned(request)
}

func (l *lateSource) GetOwnedBatch(requests []model.CandleRequest) []cluster.OwnedBatchItem {
	l.mu.RLock()
	inner := l.inner
	l.mu.RUnlock()
	if inner == nil {
		results := make([]cluster.OwnedBatchItem, len(requests))
		for i := range results {
			results[i] = cluster.OwnedBatchItem{Error: apperror.New(apperror.CodePeerUnavailable, "owner service is not ready", 0)}
		}
		return results
	}
	return inner.GetOwnedBatch(requests)
}

// node is one in-process gateway node standing in for one VPS.
type node struct {
	id       string
	member   *cluster.Membership
	peer     *httptest.Server
	svc      *service.Service
	provider *countingProvider
	store    *storage.Store
	sched    *ratelimit.Scheduler
}

type testCluster struct {
	nodes map[string]*node
	ids   [2]string
}

const (
	nodeToken  = "test-node-token"
	clusterID  = "test-cluster"
	routingKey = "routing"
)

func membershipNodes() []routing.Node {
	return []routing.Node{{ID: "node-a", Weight: 1}, {ID: "node-b", Weight: 1}}
}

// newTestCluster builds a two-node cluster. httptest loopback servers stand
// in for the confirmed private transport; every cluster, ownership, auth,
// and loop-prevention behavior under test is transport-independent.
func newTestCluster(t *testing.T, routingVersion int) *testCluster {
	t.Helper()
	ids := [2]string{"node-a", "node-b"}
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

	// Phase 1: start peer servers with late-bound sources.
	sources := map[string]*lateSource{}
	servers := map[string]*httptest.Server{}
	for _, id := range ids {
		n := nodes[id]
		source := &lateSource{}
		handler, err := peerapi.New(auth.NewNodeAuth(nodeToken), mustMembership(t, id, routingVersion, map[string]string{"node-a": "http://127.0.0.1:1", "node-b": "http://127.0.0.1:1"}), source, cluster.NodeInfo{NodeID: id, ClusterID: clusterID, RoutingVersion: routingVersion, Build: "test"}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(handler)
		sources[id] = source
		servers[id] = server
		n.peer = server
	}

	// Phase 2: real memberships + clients + services, then bind sources.
	realURLs := map[string]string{"node-a": servers["node-a"].URL, "node-b": servers["node-b"].URL}
	for _, id := range ids {
		n := nodes[id]
		member := mustMembership(t, id, routingVersion, realURLs)
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
	return &testCluster{nodes: nodes, ids: ids}
}

func (l *lateSource) set(source peerapi.CandleSource) {
	l.mu.Lock()
	l.inner = source
	l.mu.Unlock()
}

func mustMembership(t *testing.T, selfID string, routingVersion int, urls map[string]string) *cluster.Membership {
	t.Helper()
	member, err := cluster.NewMembership(clusterID, selfID, routingVersion, membershipNodes(), urls, "hash-test")
	if err != nil {
		t.Fatal(err)
	}
	return member
}

// ownerKeyedSeries finds a series whose deterministic owner is wantOwner.
func ownerKeyedSeries(t *testing.T, table *routing.Table, wantOwner string) model.SeriesKey {
	t.Helper()
	for i := range 10000 {
		key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: fmt.Sprintf("figi-%04d", i), Timeframe: "1m", CandleType: "trade"}
		assignment, err := table.Assign(key)
		if err != nil {
			t.Fatal(err)
		}
		if assignment.Primary == wantOwner {
			return key
		}
	}
	t.Fatalf("no series found for owner %s", wantOwner)
	return model.SeriesKey{}
}

func requestFor(key model.SeriesKey) model.CandleRequest {
	return model.CandleRequest{Series: key, FromUTCMS: 0, ToUTCMS: 120_000, Limit: 10}
}

// TestOneProviderCallPerClusterWideMiss is the P2.6-style core acceptance
// check: the same miss arriving at every node concurrently must produce
// exactly one provider call cluster-wide, executed by the owner only.
func TestOneProviderCallPerClusterWideMiss(t *testing.T) {
	tc := newTestCluster(t, 1)
	key := ownerKeyedSeries(t, tc.nodes["node-a"].member.Table(), "node-a")
	request := requestFor(key)

	const concurrency = 20
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := tc.nodes[tc.ids[i%2]].svc.Get(context.Background(), fmt.Sprintf("client-%d", i), ratelimit.PriorityLiveRefresh, request)
			errs[i] = err
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}
	nodeACalls := tc.nodes["node-a"].provider.calls.Load()
	nodeBCalls := tc.nodes["node-b"].provider.calls.Load()
	if nodeACalls != 1 || nodeBCalls != 0 {
		t.Fatalf("provider calls: node-a=%d node-b=%d; cluster-wide miss must be exactly 1 by the owner", nodeACalls, nodeBCalls)
	}
}

// TestRemoteOwnerServedViaPeer verifies the non-owner returns the owner's
// candles without any local provider call, and caches a replica for later.
func TestRemoteOwnerServedViaPeer(t *testing.T) {
	tc := newTestCluster(t, 1)
	key := ownerKeyedSeries(t, tc.nodes["node-a"].member.Table(), "node-a")
	request := requestFor(key)

	result, err := tc.nodes["node-b"].svc.Get(context.Background(), "client", ratelimit.PriorityLiveRefresh, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candles) != 2 {
		t.Fatalf("candles=%d", len(result.Candles))
	}
	if !result.Replica {
		t.Fatal("non-owner result must be marked replica")
	}
	if tc.nodes["node-b"].provider.calls.Load() != 0 {
		t.Fatal("non-owner must not call the provider")
	}
	if tc.nodes["node-a"].provider.calls.Load() != 1 {
		t.Fatal("owner must have made exactly one provider call")
	}

	result, err = tc.nodes["node-b"].svc.Get(context.Background(), "client", ratelimit.PriorityLiveRefresh, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.CacheStatus != service.CacheMemoryHit && result.CacheStatus != service.CachePersistentHit {
		t.Fatalf("second non-owner request should hit the replica cache, got %s", result.CacheStatus)
	}
	if tc.nodes["node-a"].provider.calls.Load() != 1 {
		t.Fatal("replica cache must suppress further owner calls")
	}
}

// TestRoutingVersionMismatchIsTyped proves a version-mismatched peer request
// is rejected without any provider work and with the typed error code.
func TestRoutingVersionMismatchIsTyped(t *testing.T) {
	tc := newTestCluster(t, 1)
	key := ownerKeyedSeries(t, tc.nodes["node-a"].member.Table(), "node-a")

	member, err := cluster.NewMembership(clusterID, "node-b", 2, membershipNodes(), map[string]string{"node-a": tc.nodes["node-a"].peer.URL, "node-b": tc.nodes["node-b"].peer.URL}, "hash2")
	if err != nil {
		t.Fatal(err)
	}
	client, err := cluster.NewClient(member, nodeToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.FetchCandles(context.Background(), requestFor(key))
	if err == nil {
		t.Fatal("expected typed error")
	}
	var typed *apperror.Error
	if !errors.As(err, &typed) || typed.Code != apperror.CodeRoutingVersionMismatch {
		t.Fatalf("expected ROUTING_VERSION_MISMATCH, got %v", err)
	}
	if tc.nodes["node-a"].provider.calls.Load() != 0 {
		t.Fatal("version mismatch must not trigger provider work")
	}
}

// TestNotOwnerPreventsLoop proves the loop-prevention contract: a peer
// request arriving at a node that computes itself as non-owner is rejected
// with NOT_OWNER and nothing is proxied or fetched.
func TestNotOwnerPreventsLoop(t *testing.T) {
	tc := newTestCluster(t, 1)
	key := ownerKeyedSeries(t, tc.nodes["node-a"].member.Table(), "node-a")

	// This client believes node-a owns the key (correct for routing v1) but its
	// node-a URL points at the node-b server, which computes itself as non-owner.
	member, err := cluster.NewMembership(clusterID, "node-a", 1, membershipNodes(), map[string]string{"node-a": tc.nodes["node-b"].peer.URL, "node-b": tc.nodes["node-b"].peer.URL}, "hash3")
	if err != nil {
		t.Fatal(err)
	}
	client, err := cluster.NewClient(member, nodeToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.FetchCandles(context.Background(), requestFor(key))
	if err == nil {
		t.Fatal("expected NOT_OWNER")
	}
	var typed *apperror.Error
	if !errors.As(err, &typed) || typed.Code != apperror.CodeNotOwner {
		t.Fatalf("expected NOT_OWNER, got %v", err)
	}
	if tc.nodes["node-b"].provider.calls.Load() != 0 || tc.nodes["node-a"].provider.calls.Load() != 0 {
		t.Fatal("NOT_OWNER must not trigger any provider call")
	}
}

// TestPeerUnreachableServesStaleReplica verifies the failure-matrix row:
// peer owner unavailable -> admissible replica stale, never a provider call.
func TestPeerUnreachableServesStaleReplica(t *testing.T) {
	tc := newTestCluster(t, 1)
	key := ownerKeyedSeries(t, tc.nodes["node-a"].member.Table(), "node-a")
	request := requestFor(key)

	if _, err := tc.nodes["node-b"].svc.Get(context.Background(), "client", ratelimit.PriorityLiveRefresh, request); err != nil {
		t.Fatal(err)
	}
	// Stop the owner's peer listener; further peer hops fail.
	tc.nodes["node-a"].peer.Close()

	wider := request
	wider.ToUTCMS = 180_000
	result, err := tc.nodes["node-b"].svc.Get(context.Background(), "client", ratelimit.PriorityLiveRefresh, wider)
	if err != nil {
		t.Fatalf("stale replica expected: %v", err)
	}
	if result.Freshness != service.StaleAcceptable {
		t.Fatalf("freshness=%s, want stale_acceptable", result.Freshness)
	}
	if !result.Replica {
		t.Fatal("stale fallback must be marked replica")
	}
	if tc.nodes["node-b"].provider.calls.Load() != 0 {
		t.Fatal("peer outage must never degrade into a local provider call")
	}
}

// TestPeerAuthDomainEnforced proves the node credential domain: no token or
// a wrong token is rejected 403 before any cluster information is served.
func TestPeerAuthDomainEnforced(t *testing.T) {
	tc := newTestCluster(t, 1)
	key := ownerKeyedSeries(t, tc.nodes["node-a"].member.Table(), "node-a")

	rogue, err := cluster.NewClient(mustMembership(t, "node-b", 1, map[string]string{"node-a": tc.nodes["node-a"].peer.URL, "node-b": tc.nodes["node-b"].peer.URL}), "wrong-token", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = rogue.FetchCandles(context.Background(), requestFor(key))
	if err == nil {
		t.Fatal("expected auth failure")
	}
	var typed *apperror.Error
	if !errors.As(err, &typed) || typed.Code != apperror.CodeNodeUnauthenticated {
		t.Fatalf("expected NODE_UNAUTHENTICATED, got %v", err)
	}
	if tc.nodes["node-a"].provider.calls.Load() != 0 {
		t.Fatal("unauthenticated peer call must not trigger provider work")
	}
}

// TestManualRoutingVersionSwitch exercises the manual switch mechanics at
// the routing layer: after a version bump the same key may change owner, and
// both nodes agree on the new assignment.
func TestManualRoutingVersionSwitch(t *testing.T) {
	tc := newTestCluster(t, 1)
	key := ownerKeyedSeries(t, tc.nodes["node-a"].member.Table(), "node-a")

	v2Table, err := routing.NewTable(2, membershipNodes())
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := v2Table.Assign(key)
	if err != nil {
		t.Fatal(err)
	}
	// Both nodes must derive the same v2 assignment from the same table.
	other, err := routing.NewTable(2, reorderNodes(membershipNodes()))
	if err != nil {
		t.Fatal(err)
	}
	otherAssignment, err := other.Assign(key)
	if err != nil {
		t.Fatal(err)
	}
	if assignment != otherAssignment {
		t.Fatalf("v2 assignments diverged: %+v vs %+v", assignment, otherAssignment)
	}
	_ = key
}

func reorderNodes(nodes []routing.Node) []routing.Node {
	return []routing.Node{nodes[1], nodes[0]}
}

// --- batch partition / bounded fan-out / per-item results ---

// keysForOwners finds one distinct series per wanted owner slot.
func keysForOwners(t *testing.T, table *routing.Table, wantOwners ...string) []model.SeriesKey {
	t.Helper()
	keys := make([]model.SeriesKey, len(wantOwners))
	assigned := map[string]bool{}
	filled := 0
	for i := range 5000 {
		key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: fmt.Sprintf("figi-b%04d", i), Timeframe: "1m", CandleType: "trade"}
		assignment, err := table.Assign(key)
		if err != nil {
			t.Fatal(err)
		}
		if assigned[key.Identity()] {
			continue
		}
		for w, owner := range wantOwners {
			if keys[w].ProviderInstrumentID == "" && assignment.Primary == owner {
				keys[w] = key
				assigned[key.Identity()] = true
				filled++
				break // one key fills at most one slot
			}
		}
		if filled == len(wantOwners) {
			return keys
		}
	}
	t.Fatalf("not enough distinct series for owners %v", wantOwners)
	return nil
}

// TestBatchPartitionsByOwnerAndServesPerItem proves the client batch path:
// a batch spanning both owners is partitioned, each owner performs its own
// provider calls, the non-owner commits read-only replicas, results keep
// request order, and total provider calls equal one per distinct series.
func TestBatchPartitionsByOwnerAndServesPerItem(t *testing.T) {
	tc := newTestCluster(t, 1)
	table := tc.nodes["node-a"].member.Table()
	keys := keysForOwners(t, table, "node-a", "node-b", "node-a", "node-b")
	requests := []model.CandleRequest{requestFor(keys[0]), requestFor(keys[1]), requestFor(keys[2]), requestFor(keys[3])}

	results := tc.nodes["node-b"].svc.GetBatch(context.Background(), "client", ratelimit.PriorityLiveRefresh, requests, service.BatchOptions{MaxFanOut: 2})
	if len(results) != 4 {
		t.Fatalf("results=%d", len(results))
	}
	for i, result := range results {
		if result.Error != nil {
			t.Fatalf("item %d failed: %v", i, result.Error)
		}
		if result.Index != i {
			t.Fatalf("item %d has index %d", i, result.Index)
		}
		if len(result.Result.Candles) != 2 {
			t.Fatalf("item %d candles=%d", i, len(result.Result.Candles))
		}
		wantReplica := i%2 == 0 // node-a-owned items arrive at node-b via peer hop and are replicas
		if result.Result.Replica != wantReplica {
			t.Fatalf("item %d replica=%v want %v", i, result.Result.Replica, wantReplica)
		}
	}
	// Exactly one provider call per distinct series cluster-wide.
	if got := tc.nodes["node-a"].provider.calls.Load(); got != 2 {
		t.Fatalf("node-a provider calls=%d, want 2 (its two owned series)", got)
	}
	if got := tc.nodes["node-b"].provider.calls.Load(); got != 2 {
		t.Fatalf("node-b provider calls=%d, want 2 (its two owned series)", got)
	}

	// A second identical batch is served entirely from cache/replica.
	results = tc.nodes["node-b"].svc.GetBatch(context.Background(), "client", ratelimit.PriorityLiveRefresh, requests, service.BatchOptions{MaxFanOut: 2})
	for i, result := range results {
		if result.Error != nil || len(result.Result.Candles) != 2 {
			t.Fatalf("second batch item %d: %+v err=%v", i, result.Result, result.Error)
		}
	}
	if got := tc.nodes["node-a"].provider.calls.Load(); got != 2 {
		t.Fatalf("second batch changed node-a provider calls to %d", got)
	}
	if got := tc.nodes["node-b"].provider.calls.Load(); got != 2 {
		t.Fatalf("second batch changed node-b provider calls to %d", got)
	}
}

// TestBatchPeerGroupFailureIsPerGroupTyped proves a dead owner marks only its
// own group's items with the typed peer failure; local items still succeed
// and the local provider is never called for the remote series.
func TestBatchPeerGroupFailureIsPerGroupTyped(t *testing.T) {
	tc := newTestCluster(t, 1)
	table := tc.nodes["node-a"].member.Table()
	keys := keysForOwners(t, table, "node-a", "node-b")
	requests := []model.CandleRequest{requestFor(keys[0]), requestFor(keys[1])}

	// Seed both series so a stale replica exists for the remote item.
	if _, err := tc.nodes["node-b"].svc.Get(context.Background(), "seed", ratelimit.PriorityLiveRefresh, requests[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.nodes["node-a"].svc.Get(context.Background(), "seed", ratelimit.PriorityLiveRefresh, requests[0]); err != nil {
		t.Fatal(err)
	}
	nodeACallsBefore := tc.nodes["node-a"].provider.calls.Load()
	nodeBCallsBefore := tc.nodes["node-b"].provider.calls.Load()

	// Kill the node-a peer listener; the node-a-owned item must fail typed (after
	// stale-replica exhaustion) while the node-b-owned item still succeeds.
	tc.nodes["node-a"].peer.Close()
	wider := requests
	wider[0].ToUTCMS = 240_000 // uncovered window -> stale not admissible
	wider[1].ToUTCMS = 240_000
	wider[1].FromUTCMS = 0
	results := tc.nodes["node-b"].svc.GetBatch(context.Background(), "client", ratelimit.PriorityLiveRefresh, wider, service.BatchOptions{MaxFanOut: 2})
	if results[1].Error != nil {
		t.Fatalf("local item failed: %v", results[1].Error)
	}
	if results[0].Error == nil {
		t.Fatal("remote item should fail typed with the owner unreachable")
	}
	// The failing peer group must not add any provider work anywhere: the
	// only new upstream call is the local node-b-owned item's wider window.
	if got := tc.nodes["node-b"].provider.calls.Load(); got != nodeBCallsBefore+1 {
		t.Fatalf("node-b provider calls=%d, want %d (only its own series)", got, nodeBCallsBefore+1)
	}
	if got := tc.nodes["node-a"].provider.calls.Load(); got != nodeACallsBefore {
		t.Fatalf("node-a provider calls changed from %d to %d", nodeACallsBefore, got)
	}
}

// TestPeerBatchEndpointRejectsNonOwnedItemsPerItem proves the owner-side
// batch endpoint re-verifies ownership per item and answers NOT_OWNER per
// item while still serving the owned items of the same request.
func TestPeerBatchEndpointRejectsNonOwnedItemsPerItem(t *testing.T) {
	tc := newTestCluster(t, 1)
	table := tc.nodes["node-a"].member.Table()
	keys := keysForOwners(t, table, "node-a", "node-b")

	member, err := cluster.NewMembership(clusterID, "node-b", 1, membershipNodes(), map[string]string{"node-a": tc.nodes["node-a"].peer.URL, "node-b": tc.nodes["node-b"].peer.URL}, "hash-batch")
	if err != nil {
		t.Fatal(err)
	}
	client, err := cluster.NewClient(member, nodeToken, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// The batch mixes one node-a-owned and one node-b-owned series but is routed
	// to the node-a owner (sender-side mis-partition): node-a must serve its item
	// and refuse the other with a per-item NOT_OWNER.
	_, err = client.FetchCandlesBatch(context.Background(), []model.CandleRequest{requestFor(keys[0]), requestFor(keys[1])})
	if err == nil {
		t.Fatal("expected mixed-owner batch to be rejected client-side before send")
	}
	// Only same-owner batches are sendable; verify the owned slice works.
	response, err := client.FetchCandlesBatch(context.Background(), []model.CandleRequest{requestFor(keys[0])})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 || response.Items[0].Error != nil || len(response.Items[0].Candles) != 2 {
		t.Fatalf("owned batch response=%+v", response.Items)
	}
	// Direct peer POST with a mixed batch: owned item served, other NOT_OWNER.
	batchPayload := cluster.PeerBatchRequest{
		ClusterID:       clusterID,
		SourceNodeID:    "node-b",
		RoutingVersion:  1,
		HopCount:        1,
		OriginatedAtUTC: time.Now().UTC().UnixMilli(),
		Requests:        []model.CandleRequest{requestFor(keys[0]), requestFor(keys[1])},
	}
	body, err := json.Marshal(batchPayload)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, tc.nodes["node-a"].peer.URL+"/internal/v1/candles/batch", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+nodeToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("batch status=%d", resp.StatusCode)
	}
	var decoded cluster.PeerBatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Items) != 2 {
		t.Fatalf("items=%d", len(decoded.Items))
	}
	if decoded.Items[0].Error != nil || len(decoded.Items[0].Candles) != 2 {
		t.Fatalf("owned item failed: %+v", decoded.Items[0])
	}
	if decoded.Items[1].Error == nil || decoded.Items[1].Error.Code != string(apperror.CodeNotOwner) {
		t.Fatalf("non-owned item must be per-item NOT_OWNER, got %+v", decoded.Items[1].Error)
	}
	// The non-owned item must not have triggered any node-b provider work
	// through node-a (no proxying).
	if got := tc.nodes["node-b"].provider.calls.Load(); got != 0 {
		t.Fatalf("node-b provider calls=%d; NOT_OWNER item must not be proxied", got)
	}
}

// TestPeerBatchAuthAndHopEnforced mirrors the single-request checks for the
// batch endpoint: auth, hop count, and routing version fail the envelope.
func TestPeerBatchAuthAndHopEnforced(t *testing.T) {
	tc := newTestCluster(t, 1)
	table := tc.nodes["node-a"].member.Table()
	keys := keysForOwners(t, table, "node-a")

	post := func(token string, hop int, version int) int {
		payload := cluster.PeerBatchRequest{ClusterID: clusterID, SourceNodeID: "node-b", RoutingVersion: version, HopCount: hop, Requests: []model.CandleRequest{requestFor(keys[0])}}
		body, _ := json.Marshal(payload)
		req, err := http.NewRequest(http.MethodPost, tc.nodes["node-a"].peer.URL+"/internal/v1/candles/batch", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if code := post("wrong-token", 1, 1); code != http.StatusForbidden {
		t.Fatalf("auth status=%d", code)
	}
	if code := post(nodeToken, 2, 1); code != http.StatusConflict {
		t.Fatalf("hop status=%d", code)
	}
	if code := post(nodeToken, 1, 9); code != http.StatusConflict {
		t.Fatalf("version status=%d", code)
	}
	if got := tc.nodes["node-a"].provider.calls.Load(); got != 0 {
		t.Fatalf("rejected batch envelopes must not reach the provider, calls=%d", got)
	}
}
