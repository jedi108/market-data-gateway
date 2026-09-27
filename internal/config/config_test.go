package config

import (
	"strconv"
	"strings"
	"testing"
)

const validYAML = `cluster:
  cluster_id: test-cluster
  node_id: node-a
  routing_version: 1
  nodes:
    - id: node-a
      weight: 1
      peer_url: https://10.0.0.1:9443
    - id: node-b
      weight: 1
      peer_url: https://10.0.0.2:9443
listeners:
  client: 127.0.0.1:8088
  peer: 10.0.0.1:9443
auth:
  node_token_env: GATEWAY_NODE_TOKEN
providers:
  tbank:
    credential_group: tbank-sandbox
    token_env: TBANK_MARKET_DATA_TOKEN
    safe_budget_per_minute: 200
    node_hard_budget_per_minute: 100
limits:
  request_timeout_ms: 1000
  max_request_limit: 100
  max_batch_items: 32
  batch_fan_out: 4
  cache_retention_ms: 60000
  storage_path: /tmp/gateway-test.sqlite
  queue_capacity: 8
  worker_count: 1
  max_retries: 1
  retry_base_delay_ms: 10
  per_client_quota_per_minute: 10
  max_tracked_clients: 4
  drain_timeout_ms: 1000
`

func TestLoadValidConfig(t *testing.T) {
	cfg, err := Load([]byte(validYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.RoutingHash == "" {
		t.Fatal("expected routing hash")
	}
}

func TestLoadRejectsPublicClientBind(t *testing.T) {
	_, err := Load([]byte("listeners:\n  client: 0.0.0.0:8088\n"))
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestLoadRejectsMissingRuntimeLimits(t *testing.T) {
	_, err := Load([]byte(`cluster: {cluster_id: x, node_id: node-a, routing_version: 1, nodes: [{id: node-a, weight: 1, peer_url: https://10.0.0.1:9443}]}
listeners: {client: 127.0.0.1:8088, peer: 10.0.0.1:9443}
providers: {tbank: {credential_group: x, token_env: TOKEN, safe_budget_per_minute: 1, node_hard_budget_per_minute: 1}}
`))
	if err == nil {
		t.Fatal("expected missing limits error")
	}
}

func TestLoadRejectsMissingLocalCoreLimits(t *testing.T) {
	_, err := Load([]byte(validYAML[:len(validYAML)-len("  drain_timeout_ms: 1000\n")]))
	if err == nil {
		t.Fatal("expected missing local-core limit error")
	}
}

func TestLoadRejectsMissingMaxTrackedClients(t *testing.T) {
	withoutMaxTrackedClients := strings.Replace(validYAML, "  max_tracked_clients: 4\n", "", 1)
	_, err := Load([]byte(withoutMaxTrackedClients))
	if err == nil {
		t.Fatal("expected missing max tracked clients error")
	}
}

// peerAddressA/B are documentation zone (RFC 1918 TEST-NET style privates)
// used by peer-bind fixtures; they never route anywhere.
const (
	peerAddressA = "10.0.0.1"
	peerAddressB = "10.0.0.2"
)

// TestRoutingHashIdenticalAcrossNodes proves the P2.1 contract: the same
// logical routing config yields the identical hash on every node regardless
// of member order, node_id, or YAML spelling of the weight.
func TestRoutingHashIdenticalAcrossNodes(t *testing.T) {
	nodeA, err := Load([]byte(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	nodeB := `cluster:
  cluster_id: test-cluster
  node_id: node-b
  routing_version: 1
  nodes:
    - id: node-b
      weight: 1.0
      peer_url: https://` + peerAddressB + `:9443
    - id: node-a
      weight: 1
      peer_url: https://` + peerAddressA + `:9443
listeners:
  client: 127.0.0.1:8088
  peer: ` + peerAddressB + `:9443
auth:
  node_token_env: GATEWAY_NODE_TOKEN
providers:
  tbank:
    credential_group: tbank-sandbox
    token_env: TBANK_MARKET_DATA_TOKEN
    safe_budget_per_minute: 200
    node_hard_budget_per_minute: 100
limits:
  request_timeout_ms: 1000
  max_request_limit: 100
  max_batch_items: 32
  batch_fan_out: 4
  cache_retention_ms: 60000
  storage_path: /tmp/gateway-test-node-b.sqlite
  queue_capacity: 8
  worker_count: 1
  max_retries: 1
  retry_base_delay_ms: 10
  per_client_quota_per_minute: 10
  max_tracked_clients: 4
  drain_timeout_ms: 1000
`
	nb, err := Load([]byte(nodeB))
	if err != nil {
		t.Fatal(err)
	}
	if nodeA.RoutingHash != nb.RoutingHash {
		t.Fatalf("routing hash diverged between nodes: %s vs %s", nodeA.RoutingHash, nb.RoutingHash)
	}
	// A different membership must change the hash.
	other := strings.Replace(validYAML, "- id: node-b", "- id: third", 1)
	changed, err := Load([]byte(other))
	if err != nil {
		t.Fatal(err)
	}
	if changed.RoutingHash == nodeA.RoutingHash {
		t.Fatal("membership change did not change routing hash")
	}
}

func TestLoadRejectsPeerBindMismatch(t *testing.T) {
	// The local node is node-a with peer_url host A, but the peer listener
	// binds the other member's address B.
	bad := strings.Replace(validYAML, "  peer: "+peerAddressA+":9443", "  peer: "+peerAddressB+":9443", 1)
	if _, err := Load([]byte(bad)); err == nil {
		t.Fatal("expected peer bind mismatch error")
	}
}

func TestLoadRejectsDuplicatePeerURL(t *testing.T) {
	bad := strings.Replace(validYAML, "https://"+peerAddressB+":9443", "https://"+peerAddressA+":9443", 1)
	if _, err := Load([]byte(bad)); err == nil {
		t.Fatal("expected duplicate peer_url error")
	}
}

// withNodeBudgets returns validYAML plus a per-node hard share map.
func withNodeBudgets(nodeAShare, nodeBShare int) string {
	return strings.Replace(validYAML,
		"    node_hard_budget_per_minute: 100\n",
		strings.Join([]string{
			"    node_hard_budget_per_minute: " + itoa(nodeAShare) + "\n",
			"    node_hard_budgets_per_minute:\n",
			"      node-a: " + itoa(nodeAShare) + "\n",
			"      node-b: " + itoa(nodeBShare) + "\n",
		}, ""),
		1)
}

func itoa(v int) string {
	return strconv.Itoa(v)
}

// TestLoadRejectsUnsafeClusterBudgetSum proves the cluster budget invariant at config
// load: sum(node shares) must not exceed the safe user budget.
func TestLoadRejectsUnsafeClusterBudgetSum(t *testing.T) {
	// safe=200, shares 120+120=240 > 200 -> reject.
	if _, err := Load([]byte(withNodeBudgets(120, 120))); err == nil {
		t.Fatal("expected cluster budget invariant error")
	}
}

func TestLoadAcceptsExactSafeBudgetSplit(t *testing.T) {
	cfg, err := Load([]byte(withNodeBudgets(100, 100)))
	if err != nil {
		t.Fatalf("exact split rejected: %v", err)
	}
	if cfg.Providers.TBank.NodeHardBudgets["node-a"] != 100 || cfg.Providers.TBank.NodeHardBudgets["node-b"] != 100 {
		t.Fatalf("shares=%+v", cfg.Providers.TBank.NodeHardBudgets)
	}
}

func TestLoadRejectsIncompleteOrMismatchedShareMap(t *testing.T) {
	// Map missing one member (only node-a present).
	dropped := strings.Replace(withNodeBudgets(100, 100), "      node-b: 100\n", "", 1)
	if _, err := Load([]byte(dropped)); err == nil {
		t.Fatal("expected incomplete share map error")
	}
	// Local scalar share disagrees with the map entry for the local node.
	mismatched := strings.Replace(withNodeBudgets(100, 100), "    node_hard_budget_per_minute: 100", "    node_hard_budget_per_minute: 90", 1)
	if _, err := Load([]byte(mismatched)); err == nil {
		t.Fatal("expected local share mismatch error")
	}
	// Share map covering a non-member id.
	alien := strings.Replace(withNodeBudgets(100, 100), "      node-b: 100", "      ghost: 100", 1)
	if _, err := Load([]byte(alien)); err == nil {
		t.Fatal("expected non-member share error")
	}
}

func TestLoadRequiresBatchBounds(t *testing.T) {
	// validYAML already carries valid bounds (32/4); removing or zeroing
	// either must fail validation.
	missing := strings.Replace(validYAML, "  max_batch_items: 32\n", "", 1)
	if _, err := Load([]byte(missing)); err == nil {
		t.Fatal("expected missing max_batch_items error")
	}
	invalid := strings.Replace(validYAML, "  max_batch_items: 32", "  max_batch_items: 0", 1)
	if _, err := Load([]byte(invalid)); err == nil {
		t.Fatal("expected invalid max_batch_items error")
	}
	missingFanOut := strings.Replace(validYAML, "  batch_fan_out: 4\n", "", 1)
	if _, err := Load([]byte(missingFanOut)); err == nil {
		t.Fatal("expected missing batch_fan_out error")
	}
}

// singleNodeLoopbackYAML is the strictly scoped GW-PUB.02 quick-start
// topology: one membership node, manual failover, no witnesses, and the
// whole peer boundary on loopback.
const singleNodeLoopbackYAML = `cluster:
  cluster_id: test-cluster
  node_id: node-a
  routing_version: 1
  failover_mode: manual
  nodes:
    - id: node-a
      weight: 1
      peer_url: https://127.0.0.1:9443
listeners:
  client: 127.0.0.1:8088
  peer: 127.0.0.1:9443
auth:
  node_token_env: GATEWAY_NODE_TOKEN
providers:
  tbank:
    credential_group: tbank-sandbox
    token_env: TBANK_MARKET_DATA_TOKEN
    safe_budget_per_minute: 600
    node_hard_budget_per_minute: 500
limits:
  request_timeout_ms: 1000
  max_request_limit: 100
  max_batch_items: 32
  batch_fan_out: 4
  cache_retention_ms: 60000
  storage_path: /tmp/gateway-test-loopback.sqlite
  queue_capacity: 8
  worker_count: 1
  max_retries: 1
  retry_base_delay_ms: 10
  per_client_quota_per_minute: 10
  max_tracked_clients: 4
  drain_timeout_ms: 1000
`

// TestLoopbackPeerAcceptedForSingleNodeManualMode proves the accepted side
// of the loopback exception: a single-node, manual (or default) failover
// cluster with no witnesses may bind and address the peer boundary on
// loopback.
func TestLoopbackPeerAcceptedForSingleNodeManualMode(t *testing.T) {
	cfg, err := Load([]byte(singleNodeLoopbackYAML))
	if err != nil {
		t.Fatalf("single-node manual loopback config rejected: %v", err)
	}
	if !isLoopback(cfg.Listeners.Peer) || !isLoopbackURL(cfg.Cluster.Nodes[0].PeerURL) {
		t.Fatalf("expected loopback peer boundary, got listener=%q peer_url=%q", cfg.Listeners.Peer, cfg.Cluster.Nodes[0].PeerURL)
	}
	// The absent failover mode must behave exactly like "manual".
	withoutMode := strings.Replace(singleNodeLoopbackYAML, "  failover_mode: manual\n", "", 1)
	if _, err := Load([]byte(withoutMode)); err != nil {
		t.Fatalf("single-node default-failover loopback config rejected: %v", err)
	}
}

// TestLoopbackPeerRejectedForMultiNodeMembership proves the exception does
// not extend to multi-node clusters: any loopback peer_url or loopback peer
// listener is rejected there, fail-closed.
func TestLoopbackPeerRejectedForMultiNodeMembership(t *testing.T) {
	// Two-node membership where one member's peer_url is loopback.
	oneLoopbackURL := strings.Replace(validYAML, "https://"+peerAddressB+":9443", "https://127.0.0.1:9443", 1)
	if _, err := Load([]byte(oneLoopbackURL)); err == nil {
		t.Fatal("expected loopback peer_url rejection for multi-node membership")
	}
	// Fully loopback two-node topology (both peer_urls and the listener).
	bothLoopback := strings.Replace(oneLoopbackURL, "https://"+peerAddressA+":9443", "https://127.0.0.1:9443", 1)
	bothLoopback = strings.Replace(bothLoopback, "  peer: "+peerAddressA+":9443", "  peer: 127.0.0.1:9443", 1)
	if _, err := Load([]byte(bothLoopback)); err == nil {
		t.Fatal("expected fully loopback two-node config rejection")
	}
	// Loopback peer listener with private membership is rejected for a
	// multi-node cluster as well.
	loopbackListener := strings.Replace(validYAML, "  peer: "+peerAddressA+":9443", "  peer: 127.0.0.1:9443", 1)
	if _, err := Load([]byte(loopbackListener)); err == nil {
		t.Fatal("expected loopback peer listener rejection for multi-node membership")
	}
}

// TestLoopbackPeerRejectedForAutomaticFailover proves automatic failover
// never gets the loopback exception, even for a single-node membership with
// a complete witness quorum: the peer boundary stays private-only.
func TestLoopbackPeerRejectedForAutomaticFailover(t *testing.T) {
	automatic := strings.Replace(singleNodeLoopbackYAML,
		"  failover_mode: manual\n",
		"  failover_mode: automatic\n"+
			"  witnesses:\n"+
			"    - id: witness-1\n"+
			"      peer_url: https://"+peerAddressA+":9445\n"+
			"    - id: witness-2\n"+
			"      peer_url: https://"+peerAddressB+":9445\n", 1)
	if _, err := Load([]byte(automatic)); err == nil {
		t.Fatal("expected automatic failover with loopback peer boundary to be rejected")
	}
}

// TestWitnessLoopbackPeerURLRejected proves the witness gate stays private
// unconditionally: a witness peer_url never receives the loopback exception.
func TestWitnessLoopbackPeerURLRejected(t *testing.T) {
	privatePeer := strings.Replace(singleNodeLoopbackYAML,
		"  peer: 127.0.0.1:9443\n", "  peer: "+peerAddressA+":9443\n", 1)
	privatePeer = strings.Replace(privatePeer, "https://127.0.0.1:9443", "https://"+peerAddressA+":9443", 1)
	privatePeer = strings.Replace(privatePeer, "  failover_mode: manual\n",
		"  failover_mode: automatic\n"+
			"  witnesses:\n"+
			"    - id: witness-1\n"+
			"      peer_url: https://127.0.0.1:9445\n"+
			"    - id: witness-2\n"+
			"      peer_url: https://"+peerAddressB+":9445\n", 1)
	if _, err := Load([]byte(privatePeer)); err == nil {
		t.Fatal("expected loopback witness peer_url rejection")
	}
}

// TestWitnessPresenceDisablesLoopbackException proves the loopback exception
// requires an empty witness list even for an otherwise eligible single-node
// manual cluster.
func TestWitnessPresenceDisablesLoopbackException(t *testing.T) {
	withWitness := strings.Replace(singleNodeLoopbackYAML, "  failover_mode: manual\n",
		"  failover_mode: manual\n"+
			"  witnesses:\n"+
			"    - id: witness-1\n"+
			"      peer_url: https://"+peerAddressA+":9445\n", 1)
	if _, err := Load([]byte(withWitness)); err == nil {
		t.Fatal("expected witness presence to disable the loopback exception")
	}
}
