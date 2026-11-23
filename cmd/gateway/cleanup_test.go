package main

import (
	"encoding/json"
	"github.com/jedi108/market-data-gateway/internal/config"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/storage"
	"strings"
	"testing"
	"time"
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

func TestCleanupReportFundingRetention(t *testing.T) {
	var cfg config.Config
	cfg.Limits.StoragePath = t.TempDir() + "/cache.db"
	cfg.Limits.CacheRetentionMS = int64(time.Hour / time.Millisecond)
	now := time.Now().UnixMilli()
	store, err := storage.Open(cfg.Limits.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	key := model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true}
	events := []model.FundingEvent{{Ts: now - 7200000, Rate: "0.001", IntervalH: 8, AvailableAt: now - 7200000, Availability: "historical_payment_assumption"}, {Ts: now, Rate: "0.002", IntervalH: 8, AvailableAt: now, Availability: "historical_payment_assumption"}}
	if err := store.Save(key, storage.Snapshot{FundingEvents: events, Metadata: storage.Metadata{Coverage: []storage.Interval{{FromUTCMS: now - 7200000, ToUTCMS: now + 1}}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report := runCleanupOnce(cfg)
	if report.ExitCode != 0 || report.FundingEventsBefore != 2 || report.FundingEventsAfter != 1 || report.DeletedFundingEvents != 1 {
		t.Fatalf("report: %+v", report)
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"funding_events_before":2`, `"funding_events_after":1`, `"deleted_funding_events":1`} {
		if !strings.Contains(string(data), field) {
			t.Fatalf("missing %s in %s", field, data)
		}
	}
}
