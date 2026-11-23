package crypto

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/httpapi"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/observability"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

func TestReconciledMarkSourceRangeAndFailures(t *testing.T) {
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	w, _ := z.Create("BTCUSDT-1h-2024-01.csv")
	fmt.Fprint(w, "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1704067200000,10,12,9,11,0,1704070799999,0,3600,0,0,0\n")
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	pin := fmt.Sprintf("%x", sha256.Sum256(b.Bytes()))
	checksum := pin + "  BTCUSDT-1h-2024-01.zip"
	for _, tc := range []struct {
		name          string
		pinned        bool
		checksum, api string
		from, to      int64
		calls         int32
		success       bool
	}{
		{"match", true, checksum, `[[1704067200000,"10","12","9","11","0",1704070799999]]`, 1704067200000, 1704070800000, 3, true},
		{"disagreement", true, checksum, `[[1704067200000,"10","12","9","10","0",1704070799999]]`, 1704067200000, 1704070800000, 3, false},
		{"wrong checksum", true, "bad", `[]`, 1704067200000, 1704070800000, 2, false},
		{"missing pin", false, checksum, `[]`, 1704067200000, 1704070800000, 0, false},
		{"partial candle", true, checksum, `[]`, 1704067200001, 1704070800000, 0, false},
		{"cross month", true, checksum, `[]`, 1706742000000, 1706749200000, 0, false},
		{"empty API", true, checksum, `[]`, 1704067200000, 1704070800000, 3, false},
		{"missing range candle", true, checksum, `[]`, 1704067200000, 1704074400000, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch {
				case strings.HasSuffix(r.URL.Path, ".CHECKSUM"):
					fmt.Fprint(w, tc.checksum)
				case strings.HasSuffix(r.URL.Path, ".zip"):
					_, _ = w.Write(b.Bytes())
				default:
					fmt.Fprint(w, tc.api)
				}
			}))
			defer srv.Close()
			s := NewReconciledMarkSource(srv.Client())
			s.archiveBase = srv.URL
			s.api.apiBase = srv.URL
			s.pin = func(_, _, _, _ string) (string, bool) { return pin, tc.pinned }
			key := model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "mark", Completeness: "reconciled", DiagnosticOnly: true}
			sched := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 3, PerClientQuotaPerMinute: 3, MaxTrackedClients: 2, QueueCapacity: 2, WorkerCount: 1, RetryBaseDelay: time.Millisecond})
			defer sched.Drain(context.Background())
			ch, err := sched.Submit(context.Background(), "client", ratelimit.PriorityLiveRefresh, func(ctx context.Context) error {
				provider := &DiagnosticProvider{mark: s}
				rows, _, err := provider.FetchCandles(ctx, model.CandleRequest{Series: key, FromUTCMS: tc.from, ToUTCMS: tc.to, Limit: 10})
				if tc.success && (err != nil || len(rows) != 1) {
					return fmt.Errorf("rows=%v error=%v", rows, err)
				}
				if !tc.success {
					var typed *apperror.Error
					if !errors.As(err, &typed) || typed.Code != apperror.CodeCoverageUnknown || typed.Retryable {
						return fmt.Errorf("expected non-retryable COVERAGE_UNKNOWN, got %v", err)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = <-ch; err != nil {
				t.Fatal(err)
			}
			if calls.Load() != tc.calls {
				t.Fatalf("calls=%d expected=%d", calls.Load(), tc.calls)
			}
			// The scheduler charges the initial submission even if validation rejects before HTTP.
			if tc.calls > 0 && sched.Snapshot().NodeBudgetUsed != int(tc.calls) {
				t.Fatalf("calls=%d budget=%d", tc.calls, sched.Snapshot().NodeBudgetUsed)
			}
		})
	}
}

func TestMarkGETReconciledFixtureCacheAndPersistence(t *testing.T) {
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	w, _ := z.Create("BTCUSDT-1h-2024-01.csv")
	fmt.Fprint(w, "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1704067200000,10,12,9,11,0,1704070799999,0,3600,0,0,0\n")
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	pin := fmt.Sprintf("%x", sha256.Sum256(b.Bytes()))
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, ".CHECKSUM"):
			fmt.Fprint(w, pin+"  BTCUSDT-1h-2024-01.zip")
		case strings.HasSuffix(r.URL.Path, ".zip"):
			_, _ = w.Write(b.Bytes())
		default:
			fmt.Fprint(w, `[[1704067200000,"10","12","9","11","0",1704070799999]]`)
		}
	}))
	defer server.Close()
	source := NewReconciledMarkSource(server.Client())
	source.archiveBase = server.URL
	source.api.apiBase = server.URL
	source.pin = func(_, _, _, _ string) (string, bool) { return pin, true }
	db := t.TempDir() + "/cache.db"
	store, err := storage.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sched := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 20, PerClientQuotaPerMinute: 20, MaxTrackedClients: 10, QueueCapacity: 10, WorkerCount: 1, RetryBaseDelay: time.Millisecond})
	defer sched.Drain(context.Background())
	key := model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "mark", Completeness: "reconciled", DiagnosticOnly: true}
	resolve := func(_, _, _ string) (model.SeriesKey, error) { return key, nil }
	recorder := observability.NewRecorder()
	makeHandler := func() *httpapi.GatewayHandler {
		svc, err := service.New(cache.New(), store, &DiagnosticProvider{mark: source}, sched, service.Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour, Recorder: recorder})
		if err != nil {
			t.Fatal(err)
		}
		h, err := httpapi.NewGatewayHandlerWithIdentity("test", func() bool { return true }, svc, resolve, 1000, 10, 1, httpapi.WithMarkResolver(resolve))
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	path := "/v1/candles?venue=binance&symbol=BTCUSDT&timeframe=1h&from_utc_ms=1704067200000&to_utc_ms=1704070800000&limit=1&include_incomplete=false&candle_type=mark&completeness=reconciled&diagnostic_only=true"
	h := makeHandler()
	for i := 0; i < 3; i++ {
		if i == 2 {
			h = makeHandler()
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"CandleType":"mark"`) || !strings.Contains(rec.Body.String(), `"diagnostic_only":true`) || !strings.Contains(rec.Body.String(), `"Close":"11"`) {
			t.Fatalf("request %d: status=%d body=%s", i, rec.Code, rec.Body.String())
		}
	}
	metrics := httptest.NewRecorder()
	h.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/v1/metrics", nil))
	for _, expected := range []string{`gateway_diagnostic_series_cache_source_total{candle_type="mark",source="refreshed"} 1`, `gateway_diagnostic_series_cache_source_total{candle_type="mark",source="memory_hit"} 1`, `gateway_diagnostic_series_cache_source_total{candle_type="mark",source="persistent_hit"} 1`} {
		if !strings.Contains(metrics.Body.String(), expected) {
			t.Fatalf("missing metric %s", expected)
		}
	}
	if calls.Load() != 3 || sched.Snapshot().NodeBudgetUsed != 3 {
		t.Fatalf("upstream calls=%d budget=%d", calls.Load(), sched.Snapshot().NodeBudgetUsed)
	}
	for _, bad := range []string{strings.Replace(path, "completeness=reconciled", "completeness=unknown", 1), strings.Replace(path, "diagnostic_only=true", "diagnostic_only=false", 1), strings.Replace(path, "1704067200000", "1704067200001", 1)} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, bad, nil))
		if rec.Code == http.StatusOK {
			t.Fatalf("bad request succeeded: %s", bad)
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("invalid request reached upstream: %d", calls.Load())
	}
}
