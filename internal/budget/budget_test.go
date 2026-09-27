package budget

import (
	"errors"
	"sync"
	"testing"
)

func shares() []Share {
	return []Share{{NodeID: "node-a", HardBudgetPerMinute: 60}, {NodeID: "node-b", HardBudgetPerMinute: 40}}
}

func TestNewAcceptsSafeSplit(t *testing.T) {
	g, err := New("cluster", "tbank-main", 100, "node-a", shares())
	if err != nil {
		t.Fatalf("safe split rejected: %v", err)
	}
	if err := g.Admit(); err != nil {
		t.Fatalf("Admit on safe config: %v", err)
	}
	snap := g.Snapshot()
	if snap.SafeBudget != 100 || snap.ConfiguredSum != 100 {
		t.Fatalf("snapshot=%+v", snap)
	}
}

func TestNewRejectsUnsafeSum(t *testing.T) {
	unsafe := []Share{{NodeID: "node-a", HardBudgetPerMinute: 60}, {NodeID: "node-b", HardBudgetPerMinute: 60}}
	if _, err := New("cluster", "tbank-main", 100, "node-a", unsafe); !errors.Is(err, ErrUnsafeClusterBudget) {
		t.Fatalf("expected ErrUnsafeClusterBudget, got %v", err)
	}
}

func TestNewRejectsMissingLocalAndInvalid(t *testing.T) {
	if _, err := New("cluster", "g", 100, "other", shares()); !errors.Is(err, ErrUnsafeClusterBudget) {
		t.Fatalf("missing local node: %v", err)
	}
	if _, err := New("", "g", 100, "node-a", shares()); !errors.Is(err, ErrUnsafeClusterBudget) {
		t.Fatalf("empty cluster id: %v", err)
	}
	if _, err := New("cluster", "g", 0, "node-a", shares()); !errors.Is(err, ErrUnsafeClusterBudget) {
		t.Fatalf("zero budget: %v", err)
	}
	if _, err := New("cluster", "g", 100, "node-a", []Share{{NodeID: "node-a", HardBudgetPerMinute: 0}}); !errors.Is(err, ErrUnsafeClusterBudget) {
		t.Fatalf("nonpositive share: %v", err)
	}
}

func TestObserveShrinkLatchesBlocked(t *testing.T) {
	g, err := New("cluster", "tbank-main", 100, "node-a", shares())
	if err != nil {
		t.Fatal(err)
	}
	// Observations at or above the configured sum must not block.
	g.ObserveShrink(120)
	g.ObserveShrink(100)
	if err := g.Admit(); err != nil {
		t.Fatalf("benign observation blocked: %v", err)
	}
	// A provider-reported budget below the configured sum blocks upstream.
	g.ObserveShrink(90)
	if err := g.Admit(); !errors.Is(err, ErrUnsafeClusterBudget) {
		t.Fatalf("shrink must block, got %v", err)
	}
	if !g.Snapshot().ObservedShrunk {
		t.Fatal("snapshot must report the latch")
	}
	// Shrinkage is monotonic: a larger later report cannot unblock.
	g.ObserveShrink(150)
	if err := g.Admit(); err == nil {
		t.Fatal("shrink latch is permanent until restart")
	}
	if g.Snapshot().ObservedBudget != 90 {
		t.Fatalf("observed=%d, want smallest 90", g.Snapshot().ObservedBudget)
	}
}

func TestObserveShrinkBelowSafeButAboveSumDoesNotBlock(t *testing.T) {
	// 60+40=100 configured, safe=200: an observed 150 is below safe but
	// still above the configured sum, so it must not block.
	g, err := New("cluster", "g", 200, "node-a", shares())
	if err != nil {
		t.Fatal(err)
	}
	g.ObserveShrink(150)
	if err := g.Admit(); err != nil {
		t.Fatalf("observation above configured sum blocked: %v", err)
	}
	if g.Snapshot().ObservedBudget != 150 {
		t.Fatalf("observed=%d", g.Snapshot().ObservedBudget)
	}
}

func TestGuardConcurrentUse(t *testing.T) {
	g, err := New("cluster", "g", 100, "node-a", shares())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g.ObserveShrink(i)
			_ = g.Admit()
			_ = g.Snapshot()
		}(i)
	}
	wg.Wait()
}
