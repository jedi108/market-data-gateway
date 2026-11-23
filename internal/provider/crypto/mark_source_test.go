package crypto

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
)

func TestBinanceMarkSourceScheduledClosedOnly(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/fapi/v1/markPriceKlines" || r.URL.Query().Get("symbol") != "BTCUSDT" || r.URL.Query().Get("interval") != "1h" || r.URL.Query().Get("endTime") != "7199999" {
			t.Errorf("unexpected request: %s", r.URL.String())
		}
		fmt.Fprint(w, `[[0,"10","12","9","11","0",3599999],[3600000,"11","13","10","12","0",7199999]]`)
	}))
	defer server.Close()
	source := NewBinanceMarkSource(server.Client())
	source.apiBase = server.URL
	source.now = func() time.Time { return time.UnixMilli(5000000) }
	key := model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "mark", Completeness: "unknown", DiagnosticOnly: true}
	if _, err := source.Fetch(context.Background(), key, 0, 7200000, 10); err == nil || calls.Load() != 0 {
		t.Fatal("unscheduled request must not reach upstream")
	}
	sched := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 1, PerClientQuotaPerMinute: 1, MaxTrackedClients: 2, QueueCapacity: 2, WorkerCount: 1, RetryBaseDelay: time.Millisecond})
	defer sched.Drain(context.Background())
	done, err := sched.Submit(context.Background(), "client", ratelimit.PriorityLiveRefresh, func(ctx context.Context) error {
		candles, err := source.Fetch(ctx, key, 0, 7200000, 2)
		if err != nil {
			return err
		}
		if len(candles) != 1 || candles[0].CloseTimeUTCMS != 3600000 || candles[0].Close != "11" {
			return fmt.Errorf("unexpected candles: %+v", candles)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || sched.Snapshot().NodeBudgetUsed != 1 {
		t.Fatalf("calls=%d budget=%d", calls.Load(), sched.Snapshot().NodeBudgetUsed)
	}
	key.Venue = "bybit"
	if _, err := source.Fetch(context.Background(), key, 0, 7200000, 10); err == nil {
		t.Fatal("unsupported venue accepted")
	}
}

func TestBinanceMarkSourceRejectsUnverifiableResponse(t *testing.T) {
	for _, body := range []string{
		`[]`,
		`[[0,"10","12","9","11","0",3599998]]`,
		`[[0,"10","12","9","11","0",3599999],[0,"10","12","9","11","0",3599999]]`,
		`[[0,"10","8","9","11","0",3599999]]`,
		`[[0,"10","12","9","11","0",3599999],[3600000,"11","13","10","12","0",7199999]]`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
			defer server.Close()
			source := NewBinanceMarkSource(server.Client())
			source.apiBase = server.URL
			source.now = func() time.Time { return time.UnixMilli(7200000) }
			key := model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "1h", CandleType: "mark", Completeness: "unknown", DiagnosticOnly: true}
			sched := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 1, PerClientQuotaPerMinute: 1, MaxTrackedClients: 2, QueueCapacity: 2, WorkerCount: 1, RetryBaseDelay: time.Millisecond})
			defer sched.Drain(context.Background())
			done, err := sched.Submit(context.Background(), "client", ratelimit.PriorityLiveRefresh, func(ctx context.Context) error {
				candles, err := source.Fetch(ctx, key, 0, 10800000, 2)
				if err == nil || len(candles) != 0 {
					return fmt.Errorf("accepted invalid or possibly truncated response: %+v, %v", candles, err)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
