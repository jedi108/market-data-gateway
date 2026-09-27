package main

import (
	"strings"
	"testing"
)

func TestCleanupLockRejectsConcurrentCleanup(t *testing.T) {
	path := t.TempDir() + "/cleanup.lock"
	first, err := acquireCleanupLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseCleanupLock(first)
	second, err := acquireCleanupLock(path)
	if err == nil {
		releaseCleanupLock(second)
		t.Fatal("expected busy cleanup lock")
	}
	if err.Error() != "cleanup lock busy" {
		t.Fatalf("unexpected lock error: %v", err)
	}
}

func TestCleanupReportDoesNotContainCredentialValues(t *testing.T) {
	report := cleanupReport{
		Host:     "node-a",
		Database: "/var/lib/market-data-gateway/node-a/state.sqlite",
		Error:    "cleanup lock busy",
		ExitCode: 75,
	}
	encoded := report.Database + report.Error
	if strings.Contains(encoded, "TBANK_MARKET_DATA_TOKEN") || strings.Contains(encoded, "GATEWAY_NODE_TOKEN") {
		t.Fatal("cleanup report contains a credential name")
	}
}
