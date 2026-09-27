// Package observability owns the bounded-cardinality metric recorder shared
// by the gateway service and HTTP boundary. Raw symbols, URLs, request IDs,
// tokens, account ids, and provider error text never become label values:
// every label passes through BoundedLabel and each family is capped, with an
// "other" bucket as the overflow destination.
package observability

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jedi108/market-data-gateway/internal/capacity"
)

const maxSeriesPerFamily = 64

// BoundedLabel normalizes a label value into a finite vocabulary.
func BoundedLabel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 32 {
		return "unknown"
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return "unknown"
		}
	}
	return value
}

type upstreamLabel struct{ Provider, Venue, Endpoint, Status string }
type latencyLabel struct{ Venue, Endpoint string }
type inflightLabel struct{ Venue, Provider string }
type integrityLabel struct{ Venue, Reason string }
type ageLabel struct{ Venue, Timeframe string }

type latencyAccumulator struct {
	count      uint64
	sumSeconds float64
}

// Recorder is safe for concurrent use.
type Recorder struct {
	mu        sync.Mutex
	upstream  map[upstreamLabel]uint64
	overflow  map[string]uint64 // status -> count past the series cap
	latency   map[latencyLabel]latencyAccumulator
	inflight  map[inflightLabel]int
	integrity map[integrityLabel]uint64
	candleAge map[ageLabel]float64
	capacity  *CapacityPlanMetrics
}

type CapacityPlanMetrics struct {
	Group, Mode, Result                string
	EffectiveRPS, AverageRPS, BurstRPS float64
	RequiredGateways                   int
}

func (r *Recorder) RecordCapacityPlan(group, mode, result string, forecast capacity.Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capacity = &CapacityPlanMetrics{Group: BoundedLabel(group), Mode: BoundedLabel(mode), Result: BoundedLabel(result), EffectiveRPS: forecast.EffectiveRPS, AverageRPS: forecast.AverageRPS, BurstRPS: forecast.BurstRPS, RequiredGateways: forecast.RequiredGateways}
}

func NewRecorder() *Recorder {
	return &Recorder{
		upstream:  make(map[upstreamLabel]uint64),
		overflow:  make(map[string]uint64),
		latency:   make(map[latencyLabel]latencyAccumulator),
		inflight:  make(map[inflightLabel]int),
		integrity: make(map[integrityLabel]uint64),
		candleAge: make(map[ageLabel]float64),
	}
}

// RecordUpstream counts one upstream provider call, its outcome, and latency.
// Each retry attempt is a separate upstream call.
func (r *Recorder) RecordUpstream(provider, venue, endpoint, status string, elapsed time.Duration) {
	label := upstreamLabel{Provider: BoundedLabel(provider), Venue: BoundedLabel(venue), Endpoint: BoundedLabel(endpoint), Status: BoundedLabel(status)}
	latencyKey := latencyLabel{Venue: label.Venue, Endpoint: label.Endpoint}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.upstream[label]; !exists && len(r.upstream) >= maxSeriesPerFamily {
		r.overflow[label.Status]++
	} else {
		r.upstream[label]++
	}
	accumulator := r.latency[latencyKey]
	accumulator.count++
	accumulator.sumSeconds += elapsed.Seconds()
	r.latency[latencyKey] = accumulator
}

// InflightInc marks one upstream call as started; InflightDec marks it done.
func (r *Recorder) InflightInc(venue, provider string) { r.adjustInflight(venue, provider, 1) }
func (r *Recorder) InflightDec(venue, provider string) { r.adjustInflight(venue, provider, -1) }

func (r *Recorder) adjustInflight(venue, provider string, delta int) {
	label := inflightLabel{Venue: BoundedLabel(venue), Provider: BoundedLabel(provider)}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.inflight[label]; !exists && delta > 0 && len(r.inflight) >= maxSeriesPerFamily {
		label = inflightLabel{Venue: label.Venue, Provider: "other"}
	}
	r.inflight[label] += delta
}

// RecordIntegrityRejection counts a payload rejected by coverage or OHLCV
// integrity rules before it could pollute the cache.
func (r *Recorder) RecordIntegrityRejection(venue, reason string) {
	label := integrityLabel{Venue: BoundedLabel(venue), Reason: BoundedLabel(reason)}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.integrity[label]; !exists && len(r.integrity) >= maxSeriesPerFamily {
		label = integrityLabel{Venue: label.Venue, Reason: "other"}
	}
	r.integrity[label]++
}

// RecordCandleAge records the age of the newest closed candle served for a
// series. It is a market-age gauge, not a fetch-age gauge; the two must never
// be merged. Session-state labelling arrives with the freshness phase.
func (r *Recorder) RecordCandleAge(venue, timeframe string, ageSeconds float64) {
	label := ageLabel{Venue: BoundedLabel(venue), Timeframe: BoundedLabel(timeframe)}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.candleAge[label]; !exists && len(r.candleAge) >= maxSeriesPerFamily {
		return
	}
	r.candleAge[label] = ageSeconds
}

type UpstreamCounter struct {
	Provider, Venue, Endpoint, Status string
	Value                             uint64
}
type LatencyAccumulator struct {
	Venue, Endpoint string
	Count           uint64
	SumSeconds      float64
}
type InflightGauge struct {
	Venue, Provider string
	Value           int
}
type IntegrityCounter struct {
	Venue, Reason string
	Value         uint64
}
type CandleAgeGauge struct {
	Venue, Timeframe string
	AgeSeconds       float64
}

// Snapshot is a deterministic point-in-time copy of every recorder family.
type Snapshot struct {
	Upstream  []UpstreamCounter
	Latency   []LatencyAccumulator
	Inflight  []InflightGauge
	Integrity []IntegrityCounter
	CandleAge []CandleAgeGauge
	Capacity  *CapacityPlanMetrics
}

func (r *Recorder) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := Snapshot{
		Upstream:  make([]UpstreamCounter, 0, len(r.upstream)+len(r.overflow)),
		Latency:   make([]LatencyAccumulator, 0, len(r.latency)),
		Inflight:  make([]InflightGauge, 0, len(r.inflight)),
		Integrity: make([]IntegrityCounter, 0, len(r.integrity)),
		CandleAge: make([]CandleAgeGauge, 0, len(r.candleAge)),
		Capacity:  r.capacity,
	}
	for label, value := range r.upstream {
		snapshot.Upstream = append(snapshot.Upstream, UpstreamCounter{Provider: label.Provider, Venue: label.Venue, Endpoint: label.Endpoint, Status: label.Status, Value: value})
	}
	for status, value := range r.overflow {
		snapshot.Upstream = append(snapshot.Upstream, UpstreamCounter{Provider: "other", Venue: "other", Endpoint: "other", Status: status, Value: value})
	}
	for label, value := range r.latency {
		snapshot.Latency = append(snapshot.Latency, LatencyAccumulator{Venue: label.Venue, Endpoint: label.Endpoint, Count: value.count, SumSeconds: value.sumSeconds})
	}
	for label, value := range r.inflight {
		snapshot.Inflight = append(snapshot.Inflight, InflightGauge{Venue: label.Venue, Provider: label.Provider, Value: value})
	}
	for label, value := range r.integrity {
		snapshot.Integrity = append(snapshot.Integrity, IntegrityCounter{Venue: label.Venue, Reason: label.Reason, Value: value})
	}
	for label, value := range r.candleAge {
		snapshot.CandleAge = append(snapshot.CandleAge, CandleAgeGauge{Venue: label.Venue, Timeframe: label.Timeframe, AgeSeconds: value})
	}
	sort.Slice(snapshot.Upstream, func(i, j int) bool { return upstreamKey(snapshot.Upstream[i]) < upstreamKey(snapshot.Upstream[j]) })
	sort.Slice(snapshot.Latency, func(i, j int) bool {
		return snapshot.Latency[i].Venue+"\x00"+snapshot.Latency[i].Endpoint < snapshot.Latency[j].Venue+"\x00"+snapshot.Latency[j].Endpoint
	})
	sort.Slice(snapshot.Inflight, func(i, j int) bool {
		return snapshot.Inflight[i].Venue+"\x00"+snapshot.Inflight[i].Provider < snapshot.Inflight[j].Venue+"\x00"+snapshot.Inflight[j].Provider
	})
	sort.Slice(snapshot.Integrity, func(i, j int) bool {
		return snapshot.Integrity[i].Venue+"\x00"+snapshot.Integrity[i].Reason < snapshot.Integrity[j].Venue+"\x00"+snapshot.Integrity[j].Reason
	})
	sort.Slice(snapshot.CandleAge, func(i, j int) bool {
		return snapshot.CandleAge[i].Venue+"\x00"+snapshot.CandleAge[i].Timeframe < snapshot.CandleAge[j].Venue+"\x00"+snapshot.CandleAge[j].Timeframe
	})
	return snapshot
}

func upstreamKey(counter UpstreamCounter) string {
	return counter.Provider + "\x00" + counter.Venue + "\x00" + counter.Endpoint + "\x00" + counter.Status
}

// WritePrometheus renders the recorder families in text exposition format.
func (s Snapshot) WritePrometheus(w io.Writer) {
	if s.Capacity != nil {
		labels := fmt.Sprintf("{credential_group=%q,mode=%q,result=%q}", s.Capacity.Group, s.Capacity.Mode, s.Capacity.Result)
		fmt.Fprintf(w, "gateway_capacity_plan_effective_rps%s %.6f\n", labels, s.Capacity.EffectiveRPS)
		fmt.Fprintf(w, "gateway_capacity_plan_average_rps%s %.6f\n", labels, s.Capacity.AverageRPS)
		fmt.Fprintf(w, "gateway_capacity_plan_burst_rps%s %.6f\n", labels, s.Capacity.BurstRPS)
		fmt.Fprintf(w, "gateway_capacity_plan_required_gateways%s %d\n", labels, s.Capacity.RequiredGateways)
	}
	fmt.Fprintln(w, "# TYPE gateway_upstream_requests_total counter")
	for _, counter := range s.Upstream {
		fmt.Fprintf(w, "gateway_upstream_requests_total{provider=%q,venue=%q,endpoint=%q,status=%q} %d\n", counter.Provider, counter.Venue, counter.Endpoint, counter.Status, counter.Value)
	}
	fmt.Fprintln(w, "# TYPE gateway_upstream_latency_seconds summary")
	for _, latency := range s.Latency {
		fmt.Fprintf(w, "gateway_upstream_latency_seconds_sum{venue=%q,endpoint=%q} %.9f\ngateway_upstream_latency_seconds_count{venue=%q,endpoint=%q} %d\n", latency.Venue, latency.Endpoint, latency.SumSeconds, latency.Venue, latency.Endpoint, latency.Count)
	}
	fmt.Fprintln(w, "# TYPE gateway_inflight_requests gauge")
	for _, gauge := range s.Inflight {
		fmt.Fprintf(w, "gateway_inflight_requests{venue=%q,provider=%q} %d\n", gauge.Venue, gauge.Provider, gauge.Value)
	}
	fmt.Fprintln(w, "# TYPE gateway_integrity_rejections_total counter")
	for _, counter := range s.Integrity {
		fmt.Fprintf(w, "gateway_integrity_rejections_total{venue=%q,reason=%q} %d\n", counter.Venue, counter.Reason, counter.Value)
	}
	fmt.Fprintln(w, "# TYPE gateway_last_closed_candle_age_seconds gauge")
	for _, gauge := range s.CandleAge {
		fmt.Fprintf(w, "gateway_last_closed_candle_age_seconds{venue=%q,timeframe=%q} %.3f\n", gauge.Venue, gauge.Timeframe, gauge.AgeSeconds)
	}
}
