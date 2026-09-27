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
