package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

func TestAdmissionReasonIsTypedAndBounded(t *testing.T) {
	for err, want := range map[error]string{
		ratelimit.ErrNodeBudget:  "node_budget",
		ratelimit.ErrClientQuota: "client_quota",
		ratelimit.ErrQueueFull:   "queue_full",
		ratelimit.ErrDraining:    "draining",
		errors.New("unrelated"):  "",
	} {
		if got := service.AdmissionReason(err); got != want {
			t.Fatalf("admissionReason(%v) = %q, want %q", err, got, want)
		}
	}
}

type handlerFakeProvider struct{ calls atomic.Int32 }

func (f *handlerFakeProvider) FetchCandles(_ context.Context, r model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	f.calls.Add(1)
	return []model.Candle{{OpenTimeUTCMS: r.FromUTCMS, CloseTimeUTCMS: r.ToUTCMS, Open: "1", High: "2", Low: "1", Close: "2", Volume: "1", IsClosed: true}}, model.ProviderMetadata{Status: "fake"}, nil
}

func TestGatewayHandlerExercisesCandleServiceAndMetrics(t *testing.T) {
	store, err := storage.Open(t.TempDir() + "/cache.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 10, PerClientQuotaPerMinute: 10, MaxTrackedClients: 10, QueueCapacity: 10, WorkerCount: 1, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := scheduler.Drain(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	provider := &handlerFakeProvider{}
	svc, err := service.New(cache.New(), store, provider, scheduler, service.Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: "1m", CandleType: "trade"}
	handler, err := NewGatewayHandler("test", func() bool { return true }, svc, func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		if venue != "tbank" || symbol != "SBER" || timeframe != "1m" {
			t.Fatalf("unexpected resolver values")
		}
		return key, nil
	}, 100)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	url := server.URL + "/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=0&to_utc_ms=60&limit=1&include_incomplete=false"
	for range 2 {
		response, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("candle status=%d", response.StatusCode)
		}
		response.Body.Close()
	}
	if provider.calls.Load() != 1 {
		t.Fatalf("provider calls=%d", provider.calls.Load())
	}
	for _, path := range []string{"/metrics", "/v1/metrics"} {
		metrics, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body := make([]byte, 4096)
		n, _ := metrics.Body.Read(body)
		metrics.Body.Close()
		metricsBody := string(body[:n])
		if metrics.StatusCode != http.StatusOK || !strings.Contains(metricsBody, "gateway_cache_memory_hits_total") || !strings.Contains(metricsBody, "gateway_client_requests_total{client=\"loopback\",venue=\"tbank\",status=\"200\"} 2") || !strings.Contains(metricsBody, "gateway_cache_requests_total{venue=\"tbank\",timeframe=\"1m\",result=\"memory_hit\"} 1") {
			t.Fatalf("%s status=%d body=%q", path, metrics.StatusCode, body[:n])
		}
	}
}

// The resolver and provider are fakes: this tests the real HTTP envelope, not
// the existence or reconciliation of a production mark/funding route.
func TestDiagnosticSeriesHTTPEnvelopeAndUnknownCoverage(t *testing.T) {
	store, err := storage.Open(t.TempDir() + "/cache.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 10, PerClientQuotaPerMinute: 10, MaxTrackedClients: 10, QueueCapacity: 10, WorkerCount: 1, RetryBaseDelay: time.Millisecond})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := scheduler.Drain(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	provider := &handlerFakeProvider{}
	svc, err := service.New(cache.New(), store, provider, scheduler, service.Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewGatewayHandler("test", func() bool { return true }, svc, func(_, symbol, _ string) (model.SeriesKey, error) {
		completeness := "reconciled"
		if symbol == "unknown" {
			completeness = "unknown"
		}
		return model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "1m", CandleType: "mark", Completeness: completeness, DiagnosticOnly: true}, nil
	}, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		symbol   string
		status   int
		contains []string
	}{
		{"reconciled", 400, []string{`"error":"UNSUPPORTED_CAPABILITY"`}},
		{"unknown", 400, []string{`"error":"UNSUPPORTED_CAPABILITY"`}},
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/candles?venue=binance&symbol="+tc.symbol+"&timeframe=1m&from_utc_ms=0&to_utc_ms=60&limit=1&include_incomplete=false", nil))
		if w.Code != tc.status {
			t.Fatalf("%s: status %d: %s", tc.symbol, w.Code, w.Body.String())
		}
		for _, part := range tc.contains {
			if !strings.Contains(w.Body.String(), part) {
				t.Fatalf("%s: missing %s in %s", tc.symbol, part, w.Body.String())
			}
		}
	}
	req := model.CandleRequest{Series: model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "1m", CandleType: "mark", Completeness: "unknown", DiagnosticOnly: true}, FromUTCMS: 0, ToUTCMS: 60, Limit: 1}
	batch := svc.GetBatch(context.Background(), "test", ratelimit.Priority(0), []model.CandleRequest{req}, service.BatchOptions{})
	var typed *apperror.Error
	if len(batch) != 1 || !errors.As(batch[0].Error, &typed) || typed.Code != apperror.CodeCoverageUnknown {
		t.Fatalf("unknown batch: %+v", batch)
	}
	for _, candleType := range []string{"funding", "mark"} {
		unlabeled := req
		unlabeled.Series.CandleType = candleType
		unlabeled.Series.Completeness = ""
		unlabeled.Series.DiagnosticOnly = false
		_, err := svc.Get(context.Background(), "test", ratelimit.Priority(0), unlabeled)
		if !errors.As(err, &typed) || typed.Code != apperror.CodeInvalidRequest {
			t.Fatalf("unlabeled %s Get error: %v", candleType, err)
		}
		items := svc.GetBatch(context.Background(), "test", ratelimit.Priority(0), []model.CandleRequest{unlabeled}, service.BatchOptions{})
		if len(items) != 1 || !errors.As(items[0].Error, &typed) || typed.Code != apperror.CodeInvalidRequest {
			t.Fatalf("unlabeled %s GetBatch: %+v", candleType, items)
		}
	}
	if provider.calls.Load() != 0 {
		t.Fatalf("invalid or unknown coverage reached provider: calls=%d", provider.calls.Load())
	}
}

func TestParseRejectsUnimplementedCandleTypes(t *testing.T) {
	h := &GatewayHandler{maxLimit: 10, resolve: func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		return model.SeriesKey{Venue: venue, MarketType: "spot", ProviderInstrumentID: symbol, Timeframe: timeframe, CandleType: "trade"}, nil
	}}
	base := "/v1/candles?venue=binance&symbol=BTCUSDT&timeframe=1m&from_utc_ms=0&to_utc_ms=60000&limit=1&include_incomplete=false"
	for _, suffix := range []string{"&candle_type=mark", "&candle_type=funding", "&candle_type=markk", "&candle_type=trade&candle_type=funding", "&candle_type="} {
		_, err := h.parse(httptest.NewRequest(http.MethodGet, base+suffix, nil))
		var typed *apperror.Error
		if !errors.As(err, &typed) || typed.Code != apperror.CodeUnsupportedCapability {
			t.Errorf("%s: got %v", suffix, err)
		}
	}
	for _, suffix := range []string{"", "&candle_type=trade"} {
		if _, err := h.parse(httptest.NewRequest(http.MethodGet, base+suffix, nil)); err != nil {
			t.Errorf("%s: %v", suffix, err)
		}
	}
}
