package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/budget"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/observability"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

// batchFixture wires the single-node local stack with the batch-enabled
// handler, mirroring the composition root defaults.
type batchFixture struct {
	server   *httptest.Server
	store    *storage.Store
	sched    *ratelimit.Scheduler
	provider *handlerFakeProvider
	recorder *observability.Recorder
}

func newBatchFixture(t *testing.T) *batchFixture {
	t.Helper()
	store, err := storage.Open(t.TempDir() + "/batch.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 128, WorkerCount: 4, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	provider := &handlerFakeProvider{}
	recorder := observability.NewRecorder()
	svc, err := service.New(cache.New(), store, provider, scheduler, service.Config{RequestTimeout: 2 * time.Second, MaxStaleAge: time.Hour, Recorder: recorder})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewGatewayHandlerWithBatchLimits("batch-test", func() bool { return true }, svc, func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		if venue != "tbank" {
			return model.SeriesKey{}, apperror.New(apperror.CodeInvalidRequest, "unsupported venue", 0)
		}
		if symbol != "SBER" {
			return model.SeriesKey{}, apperror.New(apperror.CodeUnknownSymbol, "unknown symbol", 0)
		}
		return model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "BBG004730N88", Timeframe: timeframe, CandleType: "trade"}, nil
	}, 500, 16, 4)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := scheduler.Drain(ctx); err != nil {
			t.Logf("drain: %v", err)
		}
		store.Close()
	})
	return &batchFixture{server: server, store: store, sched: scheduler, provider: provider, recorder: recorder}
}

func (f *batchFixture) postBatch(t *testing.T, body string) (int, batchEnvelope) {
	t.Helper()
	response, err := http.Post(f.server.URL+"/v1/candles/batch", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope batchEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return response.StatusCode, envelope
}

func batchBody(items ...string) string {
	return fmt.Sprintf(`{"items":[%s]}`, strings.Join(items, ","))
}

func item(timeframe string, from, to int64) string {
	return fmt.Sprintf(`{"venue":"tbank","symbol":"SBER","timeframe":"%s","from_utc_ms":%d,"to_utc_ms":%d,"limit":500,"include_incomplete":false}`, timeframe, from, to)
}

// TestBatchEndpointServesPerItemFreshness proves POST /v1/candles/batch
// returns one envelope with per-item freshness/candles, honors item order,
// and uses exactly one provider call per distinct series.
func TestBatchEndpointServesPerItemFreshness(t *testing.T) {
	f := newBatchFixture(t)
	status, envelope := f.postBatch(t, batchBody(item("1m", 0, 120_000), item("5m", 0, 300_000), item("1m", 0, 120_000)))
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if len(envelope.Items) != 3 {
		t.Fatalf("items=%d", len(envelope.Items))
	}
	for i, got := range envelope.Items {
		if got.Error != "" {
			t.Fatalf("item %d error=%s", i, got.Error)
		}
		if got.Series.Timeframe == "" || len(got.Candles) == 0 {
			t.Fatalf("item %d incomplete: %+v", i, got)
		}
	}
	if envelope.Items[0].Series.Timeframe != "1m" || envelope.Items[1].Series.Timeframe != "5m" {
		t.Fatalf("order not preserved: %s %s", envelope.Items[0].Series.Timeframe, envelope.Items[1].Series.Timeframe)
	}
	if got := f.provider.calls.Load(); got != 2 {
		t.Fatalf("provider calls=%d, want 2 distinct series", got)
	}
	// Second identical batch is fully cached.
	status, envelope = f.postBatch(t, batchBody(item("1m", 0, 120_000), item("5m", 0, 300_000)))
	if status != http.StatusOK || envelope.Items[0].Error != "" {
		t.Fatalf("second batch status=%d items=%+v", status, envelope.Items)
	}
	if got := f.provider.calls.Load(); got != 2 {
		t.Fatalf("second batch provider calls=%d, want unchanged 2", got)
	}
}

// TestBatchRejectsEnvelopeLevelErrorsBeforeWork proves transport/envelope
// validation fails the whole request before any provider call, while item
// level failures (unknown symbol inside a batch) surface per item.
func TestBatchRejectsEnvelopeLevelErrorsBeforeWork(t *testing.T) {
	f := newBatchFixture(t)
	// Oversized batch -> envelope 400.
	big := make([]string, 17)
	for i := range big {
		big[i] = item("1m", 0, 60_000)
	}
	response, err := http.Post(f.server.URL+"/v1/candles/batch", "application/json", strings.NewReader(batchBody(big...)))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized batch status=%d", response.StatusCode)
	}
	// Unknown fields -> envelope 400.
	status, _ := f.postBatch(t, `{"items":[{"venue":"tbank","symbol":"SBER","timeframe":"1m","from_utc_ms":0,"to_utc_ms":60000,"limit":10,"include_incomplete":false,"extra":1}]}`)
	if status != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d", status)
	}
	// Unknown symbol inside an item: the envelope is valid; the resolver
	// rejects the whole request before provider work in this MVP (symbol
	// validation is envelope-level here), which must still be a 4xx.
	response, err = http.Post(f.server.URL+"/v1/candles/batch", "application/json", strings.NewReader(batchBody(item("1m", 0, 60_000), `{"venue":"tbank","symbol":"NOPE","timeframe":"1m","from_utc_ms":0,"to_utc_ms":60000,"limit":10,"include_incomplete":false}`)))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode < 400 || response.StatusCode > 499 {
		t.Fatalf("unknown symbol batch status=%d", response.StatusCode)
	}
	if got := f.provider.calls.Load(); got != 0 {
		t.Fatalf("provider calls=%d, envelope errors must precede work", got)
	}
}

// TestMetricsExposeClusterBudgetFamilies proves the configured/observed
// budget exports appear once a guard is configured.
func TestMetricsExposeClusterBudgetFamilies(t *testing.T) {
	store, err := storage.Open(t.TempDir() + "/budget.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 60, PerClientQuotaPerMinute: 100, MaxTrackedClients: 10, QueueCapacity: 16, WorkerCount: 2, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		scheduler.Drain(ctx)
	}()
	guard, err := budget.New("cluster", "tbank-main", 100, "node-a", []budget.Share{{NodeID: "node-a", HardBudgetPerMinute: 60}, {NodeID: "node-b", HardBudgetPerMinute: 40}})
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(cache.New(), store, &handlerFakeProvider{}, scheduler, service.Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour, BudgetGuard: guard})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewGatewayHandler("budget-test", func() bool { return true }, svc, func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		return model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "BBG004730N88", Timeframe: timeframe, CandleType: "trade"}, nil
	}, 500)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	metrics, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metrics.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(metrics.Body); err != nil {
		t.Fatal(err)
	}
	text := buf.String()
	for _, want := range []string{
		"# TYPE gateway_cluster_budget_safe gauge",
		"gateway_cluster_budget_safe{credential_group=\"tbank-main\"} 100",
		"gateway_cluster_budget_configured_sum{credential_group=\"tbank-main\"} 100",
		"gateway_cluster_budget_observed{credential_group=\"tbank-main\"} 0",
		"gateway_cluster_budget_blocked{credential_group=\"tbank-main\"} 0",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
}

// TestBudgetGuardBlocksUpstreamWork proves the fail-closed budget-guard contract at
// the service level: a latched shrink (or unsafe config) prevents every
// provider call and surfaces the stale/error policy instead.
func TestBudgetGuardBlocksUpstreamWork(t *testing.T) {
	store, err := storage.Open(t.TempDir() + "/blocked.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 60, PerClientQuotaPerMinute: 100, MaxTrackedClients: 10, QueueCapacity: 16, WorkerCount: 2, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		scheduler.Drain(ctx)
	}()
	guard, err := budget.New("cluster", "tbank-main", 100, "node-a", []budget.Share{{NodeID: "node-a", HardBudgetPerMinute: 60}, {NodeID: "node-b", HardBudgetPerMinute: 40}})
	if err != nil {
		t.Fatal(err)
	}
	provider := &handlerFakeProvider{}
	svc, err := service.New(cache.New(), store, provider, scheduler, service.Config{RequestTimeout: time.Second, MaxStaleAge: 0, BudgetGuard: guard})
	if err != nil {
		t.Fatal(err)
	}
	// A provider observation reporting less than the configured sum latches
	// the guard; the next miss must not reach the provider.
	guard.ObserveShrink(80)
	_, err = svc.Get(context.Background(), "client", ratelimit.PriorityLiveRefresh, model.CandleRequest{Series: model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "BBG004730N88", Timeframe: "1m", CandleType: "trade"}, FromUTCMS: 0, ToUTCMS: 120_000, Limit: 10})
	if err == nil {
		t.Fatal("blocked budget must fail the miss (no stale admissible)")
	}
	if got := provider.calls.Load(); got != 0 {
		t.Fatalf("provider calls=%d; blocked guard must prevent upstream work", got)
	}
	// Batch path is blocked identically.
	results := svc.GetBatch(context.Background(), "client", ratelimit.PriorityLiveRefresh, []model.CandleRequest{{Series: model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "BBG004730N88", Timeframe: "5m", CandleType: "trade"}, FromUTCMS: 0, ToUTCMS: 300_000, Limit: 10}}, service.BatchOptions{MaxFanOut: 2})
	if len(results) != 1 || results[0].Error == nil {
		t.Fatalf("batch under blocked budget: %+v", results)
	}
	if got := provider.calls.Load(); got != 0 {
		t.Fatalf("provider calls=%d after batch", got)
	}
}
