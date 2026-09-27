package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLocalExampleConfigLoadsStrictly proves the public local quick-start
// example (config/gateway.local.example.yaml) passes the strict decoder
// (KnownFields + full Validate) and stays inside the strictly scoped
// loopback exception: single node, manual failover, loopback-only
// boundaries, local-only storage.
func TestLocalExampleConfigLoadsStrictly(t *testing.T) {
	path := filepath.Join("..", "..", "config", "gateway.local.example.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if strings.Contains(string(data), "REQUIRED") {
		t.Error("local example must be completely concrete (no REQUIRED_* placeholders)")
	}
	cfg, err := Load(data)
	if err != nil {
		t.Fatalf("strict load of %s failed: %v", path, err)
	}
	if cfg.Cluster.ClusterID != "mdg-local" {
		t.Errorf("cluster id = %q, want mdg-local", cfg.Cluster.ClusterID)
	}
	if cfg.Cluster.NodeID != "node-local" {
		t.Errorf("node id = %q, want node-local", cfg.Cluster.NodeID)
	}
	if cfg.Cluster.FailoverMode != "manual" && cfg.Cluster.FailoverMode != "" {
		t.Errorf("failover mode = %q, want manual or empty", cfg.Cluster.FailoverMode)
	}
	if len(cfg.Cluster.Nodes) != 1 {
		t.Fatalf("expected single-node membership, got %d nodes", len(cfg.Cluster.Nodes))
	}
	if cfg.Cluster.Nodes[0].ID != cfg.Cluster.NodeID {
		t.Errorf("local node %q is not the single membership member", cfg.Cluster.NodeID)
	}
	if !isLoopback(cfg.Listeners.Client) {
		t.Errorf("client listener = %q, want loopback", cfg.Listeners.Client)
	}
	if !isLoopback(cfg.Listeners.Peer) {
		t.Errorf("peer listener = %q, want loopback", cfg.Listeners.Peer)
	}
	if host, ok := peerURLHost(cfg.Cluster.Nodes[0].PeerURL); !ok || host != "127.0.0.1" {
		t.Errorf("peer_url host = %q (ok=%v), want 127.0.0.1", host, ok)
	}
	if !strings.HasPrefix(cfg.Limits.StoragePath, "build/") {
		t.Errorf("storage path = %q, want the local build/ workspace", cfg.Limits.StoragePath)
	}
	if cfg.Providers.TBank.NodeHardBudgetPerMinute > cfg.Providers.TBank.SafeBudgetPerMinute {
		t.Errorf("node hard budget %d exceeds safe budget %d",
			cfg.Providers.TBank.NodeHardBudgetPerMinute, cfg.Providers.TBank.SafeBudgetPerMinute)
	}
}
