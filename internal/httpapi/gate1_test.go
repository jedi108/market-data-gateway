package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/observability"
	tbankprovider "github.com/jedi108/market-data-gateway/internal/provider/tbank"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

// gateRegistry bounds the fixture to the canonical alias vocabulary so unknown
// symbols fail before any provider work, exactly like the composition root.
var gateRegistry = func() *tbankprovider.Registry {
	registry, err := tbankprovider.NewRegistry([]tbankprovider.Instrument{
		{CanonicalSymbol: "SBER", ProviderInstrumentID: "BBG004730N88", MarketType: "shares", Aliases: []string{"SBER/RUB"}},
	})
	if err != nil {
		panic("gate fixture registry: " + err.Error())
	}
	return registry
}()

// gateFixture wires the real local stack (cache, SQLite store, bounded
// scheduler, singleflight service) behind the gateway HTTP handler, mirroring
// the composition root minus process concerns.
type gateFixture struct {
	server    *httptest.Server
	store     *storage.Store
	scheduler *ratelimit.Scheduler
	provider  *countingProvider
	recorder  *observability.Recorder
}

type countingProvider struct {
	calls int64
	mu    sync.Mutex
	block chan struct{}
}

func (p *countingProvider) FetchCandles(ctx context.Context, r model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.block != nil {
		select {
		case <-p.block:
		case <-ctx.Done():
			return nil, model.ProviderMetadata{}, ctx.Err()
		}
	}
	var candles []model.Candle
	for open := r.FromUTCMS; open < r.ToUTCMS; open += 60_000 {
		candles = append(candles, model.Candle{OpenTimeUTCMS: open, CloseTimeUTCMS: open + 60_000, Open: "100", High: "101", Low: "99", Close: "100", Volume: "10", IsClosed: true})
	}
	return candles, model.ProviderMetadata{Status: "fake"}, nil
}

func newGateFixture(t *testing.T, path string, schedulerCfg ratelimit.Config) *gateFixture {
	t.Helper()
	store, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	scheduler := ratelimit.NewScheduler(schedulerCfg)
	provider := &countingProvider{}
	recorder := observability.NewRecorder()
	svc, err := service.New(cache.New(), store, provider, scheduler, service.Config{RequestTimeout: 2 * time.Second, MaxStaleAge: time.Hour, Recorder: recorder})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewGatewayHandler("gate-test", func() bool { return true }, svc, func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		if venue != "tbank" {
			return model.SeriesKey{}, apperror.New(apperror.CodeInvalidRequest, "unsupported venue", 0)
		}
		instrument, err := gateRegistry.Resolve(symbol)
		if err != nil {
			return model.SeriesKey{}, err
		}
		return model.SeriesKey{Venue: "tbank", MarketType: instrument.MarketType, ProviderInstrumentID: instrument.ProviderInstrumentID, Timeframe: timeframe, CandleType: "trade"}, nil
	}, 500)
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
	return &gateFixture{server: server, store: store, scheduler: scheduler, provider: provider, recorder: recorder}
}

func (f *gateFixture) candlesURL(from, to int64) string {
	return fmt.Sprintf("%s/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=%d&to_utc_ms=%d&limit=500&include_incomplete=false", f.server.URL, from, to)
}

// TestGate1HundredConcurrentIdenticalMissIsOneProviderCall is the explicit
// Gate 1 acceptance check: 100 simultaneous identical misses must produce
// exactly one provider call and 100 HTTP 200s.
func TestGate1HundredConcurrentIdenticalMissIsOneProviderCall(t *testing.T) {
	f := newGateFixture(t, t.TempDir()+"/gate.sqlite", ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 128, WorkerCount: 4, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	const concurrency = 100
	var wg sync.WaitGroup
	statuses := make([]int, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := http.Get(f.candlesURL(0, 120_000))
			if err != nil {
				t.Errorf("request %d: %v", i, err)
				return
			}
			statuses[i] = response.StatusCode
			response.Body.Close()
		}()
	}
	wg.Wait()
	ok := 0
	for _, status := range statuses {
		if status == http.StatusOK {
			ok++
		}
	}
	if ok != concurrency {
		t.Fatalf("ok=%d/%d", ok, concurrency)
	}
	f.provider.mu.Lock()
	calls := f.provider.calls
	f.provider.mu.Unlock()
	if calls != 1 {
		t.Fatalf("provider calls=%d, Gate 1 requires exactly 1", calls)
	}
}

// TestGate1CacheSurvivesRestart proves the persistent cache carries coverage
// across process boundaries: after a cold start on the same SQLite path, the
// second fixture answers from storage without a provider call.
func TestGate1CacheSurvivesRestart(t *testing.T) {
	path := t.TempDir() + "/restart.sqlite"
	first := newGateFixture(t, path, ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 16, WorkerCount: 2, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	response, err := http.Get(first.candlesURL(0, 120_000))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("warm status=%d", response.StatusCode)
	}
	response.Body.Close()

	second := newGateFixture(t, path, ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 16, WorkerCount: 2, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	response, err = http.Get(second.candlesURL(0, 120_000))
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 4096)
	n, _ := response.Body.Read(body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("restart status=%d body=%s", response.StatusCode, body[:n])
	}
	if !strings.Contains(string(body[:n]), `"cache_status":"persistent_hit"`) {
		t.Fatalf("restart served non-persistent status: %s", body[:n])
	}
	if second.provider.calls != 0 {
		t.Fatalf("restart provider calls=%d, want 0 (cache must survive restart)", second.provider.calls)
	}
}

// TestGate1OverloadBoundedReturns429WithRetryAfter proves admission control
// surfaces as HTTP 429 with a Retry-After header instead of unbounded growth.
func TestGate1OverloadBoundedReturns429WithRetryAfter(t *testing.T) {
	f := newGateFixture(t, t.TempDir()+"/overload.sqlite", ratelimit.Config{NodeBudgetPerMinute: 1, PerClientQuotaPerMinute: 1, MaxTrackedClients: 10, QueueCapacity: 1, WorkerCount: 1, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	f.provider.block = make(chan struct{})
	t.Cleanup(func() { close(f.provider.block) })

	saw429 := false
	var mu sync.Mutex
	for range 8 {
		response, err := http.Get(f.candlesURL(0, 60_000))
		if err != nil {
			t.Fatal(err)
			// request failure is not part of the overload contract; all
			// responses must arrive.
		}
		mu.Lock()
		if response.StatusCode == http.StatusTooManyRequests {
			saw429 = true
			if response.Header.Get("Retry-After") == "" {
				t.Error("429 without Retry-After header")
			}
		}
		mu.Unlock()
		response.Body.Close()
	}
	if !saw429 {
		t.Fatal("expected at least one 429 under exhausted node budget")
	}
}

// TestMetricsExposesUpstreamAndSchedulerFamilies checks the observability
// contract families are present on /metrics after real traffic.
func TestMetricsExposesUpstreamAndSchedulerFamilies(t *testing.T) {
	f := newGateFixture(t, t.TempDir()+"/metrics.sqlite", ratelimit.Config{NodeBudgetPerMinute: 100, PerClientQuotaPerMinute: 100, MaxTrackedClients: 10, QueueCapacity: 16, WorkerCount: 2, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	response, err := http.Get(f.candlesURL(0, 60_000))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	metrics, err := http.Get(f.server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metrics.Body.Close()
	body := make([]byte, 16384)
	n, _ := metrics.Body.Read(body)
	text := string(body[:n])
	for _, want := range []string{
		"gateway_upstream_requests_total{provider=\"tbank\",venue=\"tbank\",endpoint=\"get_candles\",status=\"ok\"} 1",
		"gateway_queue_depth{priority=\"live_refresh\"} 0",
		"gateway_upstream_budget_limit{venue=\"tbank\",credential_group=\"tbank\"} 100",
		"gateway_upstream_budget_remaining",
		"gateway_last_closed_candle_age_seconds{venue=\"tbank\",timeframe=\"1m\"}",
		"# TYPE gateway_integrity_rejections_total counter",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
}

// TestCandlesRejectsUnknownSymbolBeforeProviderCall proves alias resolution
// fails closed before any upstream work.
func TestCandlesRejectsUnknownSymbolBeforeProviderCall(t *testing.T) {
	f := newGateFixture(t, t.TempDir()+"/unknown.sqlite", ratelimit.Config{NodeBudgetPerMinute: 10, PerClientQuotaPerMinute: 10, MaxTrackedClients: 10, QueueCapacity: 4, WorkerCount: 1, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	response, err := http.Get(f.server.URL + "/v1/candles?venue=tbank&symbol=NOPE&timeframe=1m&from_utc_ms=0&to_utc_ms=60&limit=1&include_incomplete=false")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest && response.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("unknown symbol status=%d", response.StatusCode)
	}
	if f.provider.calls != 0 {
		t.Fatalf("provider calls=%d, unknown symbol must not reach provider", f.provider.calls)
	}
}
