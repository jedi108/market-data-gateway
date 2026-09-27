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
	metrics, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer metrics.Body.Close()
	body := make([]byte, 4096)
	n, _ := metrics.Body.Read(body)
	metricsBody := string(body[:n])
	if metrics.StatusCode != http.StatusOK || !strings.Contains(metricsBody, "gateway_cache_memory_hits_total") || !strings.Contains(metricsBody, "gateway_client_requests_total{client=\"loopback\",venue=\"tbank\",status=\"200\"} 2") || !strings.Contains(metricsBody, "gateway_cache_requests_total{venue=\"tbank\",timeframe=\"1m\",result=\"memory_hit\"} 1") {
		t.Fatalf("metrics status=%d body=%q", metrics.StatusCode, body[:n])
	}
}
