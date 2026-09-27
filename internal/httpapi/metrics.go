package httpapi

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jedi108/market-data-gateway/internal/observability"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
)

// gatewayMetrics bounds label cardinality: client identity is intentionally
// normalized to loopback, while the operational venue/timeframe vocabulary is
// capped. Raw symbols, URLs, request IDs, and provider errors never become
// metric labels.
type gatewayMetrics struct {
	mu             sync.Mutex
	requests       map[requestLabel]uint64
	cache          map[cacheLabel]uint64
	singleflight   map[cacheLabel]uint64
	stale          map[staleLabel]uint64
	latencyCount   uint64
	latencySeconds float64
}

type requestLabel struct{ Venue, Status string }
type cacheLabel struct{ Venue, Timeframe, Result string }
type staleLabel struct{ Venue, Reason string }

const maxMetricSeries = 64

func newGatewayMetrics() *gatewayMetrics {
	return &gatewayMetrics{
		requests:     make(map[requestLabel]uint64),
		cache:        make(map[cacheLabel]uint64),
		singleflight: make(map[cacheLabel]uint64),
		stale:        make(map[staleLabel]uint64),
	}
}

func (m *gatewayMetrics) recordRequest(venue, status string, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	label := requestLabel{Venue: observability.BoundedLabel(venue), Status: observability.BoundedLabel(status)}
	if _, exists := m.requests[label]; exists || len(m.requests) < maxMetricSeries {
		m.requests[label]++
	} else {
		m.requests[requestLabel{Venue: "other", Status: label.Status}]++
	}
	m.latencyCount++
	m.latencySeconds += elapsed.Seconds()
}

func (m *gatewayMetrics) recordResult(venue, timeframe, cacheStatus, freshness string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	label := cacheLabel{Venue: observability.BoundedLabel(venue), Timeframe: observability.BoundedLabel(timeframe), Result: observability.BoundedLabel(cacheStatus)}
	if _, exists := m.cache[label]; exists || len(m.cache) < maxMetricSeries {
		m.cache[label]++
	} else {
		label.Venue, label.Timeframe = "other", "other"
		m.cache[label]++
	}
	if cacheStatus == "singleflight_join" || cacheStatus == "refreshed" {
		result := "join"
		if cacheStatus == "refreshed" {
			result = "start"
		}
		singleflightLabel := cacheLabel{Venue: label.Venue, Timeframe: label.Timeframe, Result: result}
		m.singleflight[singleflightLabel]++
	}
	if freshness == "stale_acceptable" {
		m.stale[staleLabel{Venue: label.Venue, Reason: "upstream_error"}]++
	}
}

func (m *gatewayMetrics) writePrometheus(w io.Writer, cache serviceMetrics, scheduler ratelimit.Snapshot, recorder *observability.Recorder, budgetSnap budgetSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Fprintln(w, "# TYPE gateway_client_requests_total counter")
	for _, label := range sortedRequestLabels(m.requests) {
		fmt.Fprintf(w, "gateway_client_requests_total{client=\"loopback\",venue=\"%s\",status=\"%s\"} %d\n", label.Venue, label.Status, m.requests[label])
	}
	fmt.Fprintln(w, "# TYPE gateway_cache_requests_total counter")
	for _, label := range sortedCacheLabels(m.cache) {
		fmt.Fprintf(w, "gateway_cache_requests_total{venue=\"%s\",timeframe=\"%s\",result=\"%s\"} %d\n", label.Venue, label.Timeframe, label.Result, m.cache[label])
	}
	fmt.Fprintln(w, "# TYPE gateway_singleflight_total counter")
	for _, label := range sortedCacheLabels(m.singleflight) {
		fmt.Fprintf(w, "gateway_singleflight_total{venue=\"%s\",timeframe=\"%s\",result=\"%s\"} %d\n", label.Venue, label.Timeframe, label.Result, m.singleflight[label])
	}
	fmt.Fprintln(w, "# TYPE gateway_stale_responses_total counter")
	for _, label := range sortedStaleLabels(m.stale) {
		fmt.Fprintf(w, "gateway_stale_responses_total{venue=\"%s\",reason=\"%s\"} %d\n", label.Venue, label.Reason, m.stale[label])
	}
	fmt.Fprintln(w, "# TYPE gateway_client_latency_seconds summary")
	fmt.Fprintf(w, "gateway_client_latency_seconds_sum %.9f\ngateway_client_latency_seconds_count %d\n", m.latencySeconds, m.latencyCount)
	fmt.Fprintf(w, "gateway_cache_memory_hits_total %d\ngateway_cache_memory_misses_total %d\ngateway_cache_singleflight_joins_total %d\ngateway_cache_persistent_hits_total %d\ngateway_cache_replica_responses_total %d\ngateway_stale_fallbacks_total %d\n", cache.MemoryHits, cache.MemoryMisses, cache.SingleflightJoins, cache.PersistentHits, cache.ReplicaResponses, cache.StaleResponses)
	fmt.Fprintln(w, "# TYPE gateway_queue_depth gauge")
	fmt.Fprintf(w, "gateway_queue_depth{priority=\"live_refresh\"} %d\ngateway_queue_depth{priority=\"warmup\"} %d\n", scheduler.QueueDepthLiveRefresh, scheduler.QueueDepthWarmup)
	fmt.Fprintln(w, "# TYPE gateway_inflight_scheduled_requests gauge")
	fmt.Fprintf(w, "gateway_inflight_scheduled_requests %d\n", scheduler.InFlight)
	fmt.Fprintln(w, "# TYPE gateway_upstream_budget_limit gauge")
	fmt.Fprintf(w, "gateway_upstream_budget_limit{venue=\"tbank\",credential_group=\"tbank\"} %d\n", scheduler.NodeBudgetLimit)
	fmt.Fprintln(w, "# TYPE gateway_upstream_budget_remaining gauge")
	fmt.Fprintf(w, "gateway_upstream_budget_remaining{venue=\"tbank\",credential_group=\"tbank\"} %d\n", scheduler.NodeBudgetLimit-scheduler.NodeBudgetUsed)
	fmt.Fprintln(w, "# TYPE gateway_tracked_clients gauge")
	fmt.Fprintf(w, "gateway_tracked_clients %d\n", scheduler.TrackedClients)
	// Task 109 admission observability: admitted/rejected jobs by typed
	// reason — bounded label vocabulary, no client identity.
	fmt.Fprintln(w, "# TYPE gateway_admission_total counter")
	fmt.Fprintf(w, "gateway_admission_total{result=\"admitted\"} %d\n", scheduler.Admitted)
	fmt.Fprintf(w, "gateway_admission_total{result=\"rejected\",reason=\"node_budget\"} %d\n", scheduler.RejectedNodeBudget)
	fmt.Fprintf(w, "gateway_admission_total{result=\"rejected\",reason=\"client_quota\"} %d\n", scheduler.RejectedClientQuota)
	fmt.Fprintf(w, "gateway_admission_total{result=\"rejected\",reason=\"queue_full\"} %d\n", scheduler.RejectedQueueFull)
	fmt.Fprintf(w, "gateway_admission_total{result=\"rejected\",reason=\"tracking\"} %d\n", scheduler.RejectedTracking)
	fmt.Fprintf(w, "gateway_admission_total{result=\"rejected\",reason=\"draining\"} %d\n", scheduler.RejectedDraining)
	fmt.Fprintln(w, "# TYPE gateway_scheduler_retries_skipped_budget_total counter")
	fmt.Fprintf(w, "gateway_scheduler_retries_skipped_budget_total %d\n", scheduler.RetriesSkippedBudget)
	// Task 109 cache-first counters: stale-while-revalidate queue and
	// deferred/warmup outcomes.
	fmt.Fprintln(w, "# TYPE gateway_refresh_queued_total counter")
	fmt.Fprintf(w, "gateway_refresh_queued_total %d\ngateway_refresh_dropped_total %d\ngateway_refresh_deferred_total %d\n", cache.RefreshQueued, cache.RefreshDropped, cache.RefreshDeferred)
	fmt.Fprintln(w, "# TYPE gateway_warmup_series_total counter")
	fmt.Fprintf(w, "gateway_warmup_series_total{result=\"completed\"} %d\ngateway_warmup_series_total{result=\"skipped_covered\"} %d\n", cache.WarmupCompleted, cache.WarmupSkipped)
	// Configured vs observed cluster budget families. The configured
	// views are fixed per routing/config version; the observed view can only
	// shrink while the process runs.
	if budgetSnap.Present {
		fmt.Fprintln(w, "# TYPE gateway_cluster_budget_safe gauge")
		fmt.Fprintf(w, "gateway_cluster_budget_safe{credential_group=\"%s\"} %d\n", budgetSnap.CredentialGroup, budgetSnap.SafeBudget)
		fmt.Fprintln(w, "# TYPE gateway_cluster_budget_configured_sum gauge")
		fmt.Fprintf(w, "gateway_cluster_budget_configured_sum{credential_group=\"%s\"} %d\n", budgetSnap.CredentialGroup, budgetSnap.ConfiguredSum)
		fmt.Fprintln(w, "# TYPE gateway_cluster_budget_observed gauge")
		fmt.Fprintf(w, "gateway_cluster_budget_observed{credential_group=\"%s\"} %d\n", budgetSnap.CredentialGroup, budgetSnap.ObservedBudget)
		fmt.Fprintln(w, "# TYPE gateway_cluster_budget_blocked gauge")
		blocked := 0
		if budgetSnap.Blocked {
			blocked = 1
		}
		fmt.Fprintf(w, "gateway_cluster_budget_blocked{credential_group=\"%s\"} %d\n", budgetSnap.CredentialGroup, blocked)
	}
	if recorder != nil {
		recorder.Snapshot().WritePrometheus(w)
	}
}

// budgetSnapshot is the metrics-facing cluster budget view.
type budgetSnapshot struct {
	Present         bool
	CredentialGroup string
	SafeBudget      int
	ConfiguredSum   int
	ObservedBudget  int
	Blocked         bool
}

type serviceMetrics struct {
	MemoryHits, MemoryMisses, SingleflightJoins, PersistentHits, ReplicaResponses, StaleResponses uint64
	RefreshQueued, RefreshDropped, RefreshDeferred                                                uint64
	WarmupCompleted, WarmupSkipped                                                                uint64
}

func sortedRequestLabels(values map[requestLabel]uint64) []requestLabel {
	keys := make([]requestLabel, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return strconv.Quote(keys[i].Venue)+strconv.Quote(keys[i].Status) < strconv.Quote(keys[j].Venue)+strconv.Quote(keys[j].Status)
	})
	return keys
}
func sortedCacheLabels(values map[cacheLabel]uint64) []cacheLabel {
	keys := make([]cacheLabel, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Venue+"\x00"+keys[i].Timeframe+"\x00"+keys[i].Result < keys[j].Venue+"\x00"+keys[j].Timeframe+"\x00"+keys[j].Result
	})
	return keys
}
func sortedStaleLabels(values map[staleLabel]uint64) []staleLabel {
	keys := make([]staleLabel, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Venue+"\x00"+keys[i].Reason < keys[j].Venue+"\x00"+keys[j].Reason })
	return keys
}
