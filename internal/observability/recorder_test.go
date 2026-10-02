package observability

import (
	"strings"
	"testing"
	"time"
)

func TestRecorderBoundedLabelVocabulary(t *testing.T) {
	cases := map[string]string{
		"tbank":                  "tbank",
		"  node-b ":              "node-b",
		"":                       "unknown",
		"SBER/RUB":               "unknown", // raw symbols never become labels
		"http://10.0.0.1:9443/x": "unknown",
		strings.Repeat("a", 40):  "unknown",
		"ok-value_1":             "ok-value_1",
	}
	for input, want := range cases {
		if got := BoundedLabel(input); got != want {
			t.Fatalf("BoundedLabel(%q)=%q want %q", input, got, want)
		}
	}
}

func TestRecorderUpstreamLatencyInflightAndOverflow(t *testing.T) {
	r := NewRecorder()
	r.RecordUpstream("tbank", "tbank", "get_candles", "ok", 150*time.Millisecond)
	r.RecordUpstream("tbank", "tbank", "get_candles", "ok", 50*time.Millisecond)
	r.RecordUpstream("tbank", "tbank", "get_candles", "resource_exhausted", time.Second)
	r.InflightInc("tbank", "tbank")
	r.InflightInc("tbank", "tbank")
	r.InflightDec("tbank", "tbank")
	snapshot := r.Snapshot()
	if len(snapshot.Upstream) != 2 {
		t.Fatalf("upstream series=%d want 2", len(snapshot.Upstream))
	}
	found := map[string]uint64{}
	for _, counter := range snapshot.Upstream {
		found[counter.Status] = counter.Value
	}
	if found["ok"] != 2 || found["resource_exhausted"] != 1 {
		t.Fatalf("upstream counts=%v", found)
	}
	for _, latency := range snapshot.Latency {
		if latency.Count != 3 || latency.SumSeconds < 1.19 || latency.SumSeconds > 1.21 {
			t.Fatalf("latency accumulator=%+v", latency)
		}
	}
	if len(snapshot.Inflight) != 1 || snapshot.Inflight[0].Value != 1 {
		t.Fatalf("inflight=%+v", snapshot.Inflight)
	}
}

func TestRecorderOverflowFoldsIntoOtherBucket(t *testing.T) {
	r := NewRecorder()
	for i := range maxSeriesPerFamily + 10 {
		r.RecordUpstream("tbank", "tbank", "endpoint_"+string(rune('a'+i%26))+string(rune('0'+i/26)), "ok", time.Millisecond)
	}
	snapshot := r.Snapshot()
	otherTotal := uint64(0)
	series := 0
	total := uint64(0)
	for _, counter := range snapshot.Upstream {
		total += counter.Value
		if counter.Provider == "other" && counter.Venue == "other" && counter.Endpoint == "other" {
			otherTotal += counter.Value
		} else {
			series++
		}
	}
	if otherTotal != 10 {
		t.Fatalf("overflow bucket=%d want 10", otherTotal)
	}
	if series != maxSeriesPerFamily {
		t.Fatalf("capped series=%d want %d", series, maxSeriesPerFamily)
	}
	if total != maxSeriesPerFamily+10 {
		t.Fatalf("total=%d", total)
	}
}

func TestRecorderSnapshotDeterministicOrder(t *testing.T) {
	r := NewRecorder()
	r.RecordUpstream("tbank", "tbank", "get_candles", "ok", time.Millisecond)
	r.RecordUpstream("tbank", "tbank", "get_candles", "unavailable", time.Millisecond)
	r.RecordUpstream("tbank", "tbank", "get_candles", "resource_exhausted", time.Millisecond)
	first := r.Snapshot()
	for range 5 {
		if got := r.Snapshot(); upstreamKeyList(got) != upstreamKeyList(first) {
			t.Fatal("snapshot ordering is not deterministic")
		}
	}
}

func upstreamKeyList(snapshot Snapshot) string {
	parts := make([]string, 0, len(snapshot.Upstream))
	for _, counter := range snapshot.Upstream {
		parts = append(parts, upstreamKey(counter))
	}
	return strings.Join(parts, ",")
}

func TestSnapshotWritePrometheusContainsContractFamilies(t *testing.T) {
	r := NewRecorder()
	r.RecordUpstream("tbank", "tbank", "get_candles", "ok", time.Second)
	r.RecordIntegrityRejection("tbank", "incomplete_coverage")
	r.RecordCandleAge("tbank", "1m", 42.5)
	var out strings.Builder
	r.Snapshot().WritePrometheus(&out)
	body := out.String()
	for _, want := range []string{
		"# TYPE gateway_upstream_requests_total counter",
		`gateway_upstream_requests_total{provider="tbank",venue="tbank",endpoint="get_candles",status="ok"} 1`,
		"# TYPE gateway_inflight_requests gauge",
		"# TYPE gateway_integrity_rejections_total counter",
		`gateway_integrity_rejections_total{venue="tbank",reason="incomplete_coverage"} 1`,
		"# TYPE gateway_last_closed_candle_age_seconds gauge",
		`gateway_last_closed_candle_age_seconds{venue="tbank",timeframe="1m"} 42.500`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("prometheus body missing %q in:\n%s", want, body)
		}
	}
}

// TestRecorderFuturesRegistryAndSourceFamilies pins the F20 families: the
// market-age and fetch-age gauges stay separate, cache sources split by
// market type, and the registry gauge set carries active contracts, refresh
// count, and schema version.
func TestRecorderFuturesRegistryAndSourceFamilies(t *testing.T) {
	r := NewRecorder()
	r.RecordCandleAge("tbank", "1m", 42.5)
	r.RecordSourceAge("tbank", "1m", 3.25)
	r.RecordSeriesSource("tbank", "futures", "memory_hit")
	r.RecordSeriesSource("tbank", "futures", "memory_hit")
	r.RecordSeriesSource("tbank", "shares", "persistent_hit")
	r.RecordRegistryState("tbank", RegistrySchemaVersionForTest, 2, []RegistryInstrument{
		{MarketType: "shares", InstrumentType: "share", Symbol: "SBER"},
		{MarketType: "futures", InstrumentType: "future", Symbol: "GOLD-26.12", ExpirationUTCMS: 1797043200000},
		{MarketType: "futures", InstrumentType: "future", Symbol: "GOLD-27.03", ExpirationUTCMS: 1809609600000},
	})
	var out strings.Builder
	r.Snapshot().WritePrometheus(&out)
	body := out.String()
	for _, want := range []string{
		// Fetch-provenance age is its own family, never merged into the
		// market-age gauge.
		`gateway_last_closed_candle_age_seconds{venue="tbank",timeframe="1m"} 42.500`,
		`gateway_series_source_age_seconds{venue="tbank",timeframe="1m"} 3.250`,
		`gateway_series_cache_source_total{venue="tbank",market_type="futures",source="memory_hit"} 2`,
		`gateway_series_cache_source_total{venue="tbank",market_type="shares",source="persistent_hit"} 1`,
		// Registry family: dots in contract codes fold to '-', unknown classes
		// stay bounded, expiry is exposed in epoch seconds.
		`gateway_registry_instruments{venue="tbank",market_type="shares",instrument_type="share",symbol="SBER"} 1`,
		`gateway_registry_instruments{venue="tbank",market_type="futures",instrument_type="future",symbol="GOLD-26-12"} 1`,
		`gateway_registry_instruments{venue="tbank",market_type="futures",instrument_type="future",symbol="GOLD-27-03"} 1`,
		`gateway_registry_active_contract_expiry_epoch_seconds{venue="tbank",market_type="futures",instrument_type="future",symbol="GOLD-26-12"} 1797043200`,
		"gateway_registry_refreshes_total 2",
		"gateway_registry_schema_version 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("prometheus body missing %q in:\n%s", want, body)
		}
	}
	// The market-age gauge must not have absorbed the fetch age.
	if strings.Contains(body, "gateway_last_closed_candle_age_seconds{venue=\"tbank\",timeframe=\"1m\"} 3.250") {
		t.Fatalf("fetch age leaked into the market-age family:\n%s", body)
	}
}

// RegistrySchemaVersionForTest decouples the recorder test from the provider
// package constant while pinning the schema version the families expose.
const RegistrySchemaVersionForTest = 1

// TestRecorderRegistryStateIsIdempotentSnapshotReplacement proves re-recording
// the same state is stable and re-recording after a roll replaces the gauge
// set (no stale contracts linger once dropped from the registry).
func TestRecorderRegistryStateIsIdempotentSnapshotReplacement(t *testing.T) {
	r := NewRecorder()
	december := []RegistryInstrument{{MarketType: "futures", InstrumentType: "future", Symbol: "GOLD-26.12", ExpirationUTCMS: 1797043200000}}
	r.RecordRegistryState("tbank", 1, 0, december)
	first := r.Snapshot()
	r.RecordRegistryState("tbank", 1, 0, december)
	second := r.Snapshot()
	if len(first.Registry.Instruments) != 1 || len(second.Registry.Instruments) != 1 {
		t.Fatalf("idempotent re-record changed cardinality: %d -> %d", len(first.Registry.Instruments), len(second.Registry.Instruments))
	}
	march := []RegistryInstrument{{MarketType: "futures", InstrumentType: "future", Symbol: "GOLD-27.03", ExpirationUTCMS: 1809609600000}}
	r.RecordRegistryState("tbank", 1, 1, march)
	third := r.Snapshot()
	if len(third.Registry.Instruments) != 1 || third.Registry.Instruments[0].Symbol != "GOLD-27-03" || third.Registry.Refreshes != 1 {
		t.Fatalf("registry state was not replaced wholesale: %+v", third.Registry)
	}
}
