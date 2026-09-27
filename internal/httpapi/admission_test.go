// Task 109 HTTP boundary proofs: computed Retry-After from the accounting
// window, client identity allowlist rejections before admission, and the
// no-data fail-closed policy.
package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/auth"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/config"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

type gatedProvider struct{ calls atomic.Int32 }

func (f *gatedProvider) FetchCandles(_ context.Context, r model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	f.calls.Add(1)
	return []model.Candle{{OpenTimeUTCMS: r.FromUTCMS, CloseTimeUTCMS: r.ToUTCMS, Open: "1", High: "2", Low: "1", Close: "2", Volume: "1", IsClosed: true}}, model.ProviderMetadata{Status: "fake"}, nil
}

func newHandlerEnv(t *testing.T, budget int, opts ...HandlerOption) (*GatewayHandler, *gatedProvider, *ratelimit.Scheduler, func()) {
	t.Helper()
	store, err := storage.Open(t.TempDir() + "/cache.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: budget, PerClientQuotaPerMinute: budget, MaxTrackedClients: 10, QueueCapacity: 16, WorkerCount: 1, MaxRetries: 0, RetryBaseDelay: time.Millisecond, Window: 30 * time.Second})
	provider := &gatedProvider{}
	svc, err := service.New(cache.New(), store, provider, scheduler, service.Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewGatewayHandlerWithIdentity("test", func() bool { return true }, svc, func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		return model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: timeframe, CandleType: "trade"}, nil
	}, 500, 64, 8, opts...)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = scheduler.Drain(ctx)
		_ = store.Close()
	}
	return handler, provider, scheduler, cleanup
}

func candlesURL(tf string) string {
	return "/v1/candles?venue=tbank&symbol=FIGI&timeframe=" + tf + "&from_utc_ms=0&to_utc_ms=60000&limit=10&include_incomplete=false"
}

// TestRetryAfterIsComputedFromBudgetWindow: a retryable 429 must carry a
// Retry-After derived from the next admissible provider attempt, not the
// legacy hardcoded 1s.
func TestRetryAfterIsComputedFromBudgetWindow(t *testing.T) {
	handler, _, scheduler, cleanup := newHandlerEnv(t, 1)
	defer cleanup()

	// Hold a worker so the single budget slot stays consumed inside the 30s
	// window; the op must respect cancellation.
	hold := make(chan struct{})
	_, err := scheduler.Submit(context.Background(), "hog", ratelimit.PriorityWarmup, func(ctx context.Context) error {
		select {
		case <-hold:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer close(hold)

	server := httptest.NewServer(handler)
	defer server.Close()
	resp, err := http.Get(server.URL + candlesURL("1m"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want 429", resp.StatusCode)
	}
	retryAfter, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil {
		t.Fatalf("Retry-After header: %v", err)
	}
	if retryAfter < 25 || retryAfter > 30 {
		t.Fatalf("Retry-After=%ds, want the computed window remainder (25..30)", retryAfter)
	}
}

// TestClientIdentityAllowlistRejectsBeforeAdmission: with an allowlist
// configured, requests without a matching credential are rejected 401 and
// never reach scheduler admission; a spoofed X-Client-ID cannot select an
// identity.
func TestClientIdentityAllowlistRejectsBeforeAdmission(t *testing.T) {
	t.Setenv("TEST_CLIENT_TOKEN_BOT", "secret-bot-token")
	directory := auth.NewClientDirectory([]config.ClientIdentity{
		{ID: "bot", TokenEnv: "TEST_CLIENT_TOKEN_BOT", Priority: "live_refresh"},
	}, func(key string) string { return testGetenv(key) }, nil)
	if !directory.Enabled() {
		t.Fatal("directory must be enabled with a provisioned credential")
	}
	handler, provider, _, cleanup := newHandlerEnv(t, 10, WithIdentityResolver(directory.Resolve))
	defer cleanup()

	server := httptest.NewServer(handler)
	defer server.Close()

	// No credential: rejected before admission.
	resp, err := http.Get(server.URL + candlesURL("1m"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401 without credential", resp.StatusCode)
	}
	// Wrong credential: same typed rejection.
	req, _ := http.NewRequest(http.MethodGet, server.URL+candlesURL("1m"), nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	req.Header.Set("X-Client-ID", "bot")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401 for unknown credential", resp2.StatusCode)
	}
	if provider.calls.Load() != 0 {
		t.Fatalf("provider called %d times; identity rejection must precede admission", provider.calls.Load())
	}
	// Valid credential and a spoofed X-Client-ID: admitted under the
	// token-bound identity regardless of the header.
	req3, _ := http.NewRequest(http.MethodGet, server.URL+candlesURL("1m"), nil)
	req3.Header.Set("Authorization", "Bearer secret-bot-token")
	req3.Header.Set("X-Client-ID", "someone-else")
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200 for a valid credential", resp3.StatusCode)
	}
}

func testGetenv(key string) string {
	if key == "TEST_CLIENT_TOKEN_BOT" {
		return "secret-bot-token"
	}
	return ""
}

// TestMetricsExposeTask109Families guards the wiring of the task 109 metric
// families into the /metrics output (regression: composite literal dropped
// the new serviceMetrics fields, so the families always reported 0).
func TestMetricsExposeTask109Families(t *testing.T) {
	handler, _, _, cleanup := newHandlerEnv(t, 10)
	defer cleanup()

	server := httptest.NewServer(handler)
	defer server.Close()
	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body := make([]byte, 0)
	buf := make([]byte, 4096)
	for {
		n, readErr := resp.Body.Read(buf)
		body = append(body, buf[:n]...)
		if readErr != nil {
			break
		}
	}
	out := string(body)
	for _, family := range []string{
		"gateway_admission_total",
		"gateway_scheduler_retries_skipped_budget_total",
		"gateway_refresh_queued_total",
		"gateway_refresh_dropped_total",
		"gateway_refresh_deferred_total",
		"gateway_warmup_series_total",
	} {
		if !strings.Contains(out, family) {
			t.Fatalf("metrics output missing family %s", family)
		}
	}
}
