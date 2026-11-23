package crypto

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/ratelimit"
)

func TestBinanceFundingSourceScheduledReconciliation(t *testing.T) {
	data, checksum, pin := fundingFixture(t, "BTCUSDT-fundingRate-2024-01.csv", "calc_time,funding_interval_hours,last_funding_rate\n1704067200000,8,0.0001\n1704096000000,4,-0.0002\n")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, ".CHECKSUM"):
			fmt.Fprint(w, checksum)
		case strings.HasSuffix(r.URL.Path, ".zip"):
			_, _ = w.Write(data)
		case r.URL.Path == "/fapi/v1/fundingRate":
			fmt.Fprint(w, `[{"symbol":"BTCUSDT","fundingTime":1704067200000,"fundingRate":"0.0001"},{"symbol":"BTCUSDT","fundingTime":1704096000000,"fundingRate":"-0.0002"}]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	source := NewBinanceFundingSource(server.Client())
	source.archiveBase, source.apiBase = server.URL, server.URL
	source.pin = func(_, _, _ string) (string, bool) { return pin, true }
	from, to := int64(1704067200000), int64(1704124800000)
	if _, err := source.Fetch(context.Background(), "BTCUSDT", from, to); err == nil || calls.Load() != 0 {
		t.Fatal("unscheduled fetch must fail before HTTP")
	}
	cfg := ratelimit.Config{NodeBudgetPerMinute: 3, PerClientQuotaPerMinute: 3, MaxTrackedClients: 2, QueueCapacity: 2, WorkerCount: 1, RetryBaseDelay: time.Millisecond, Window: time.Minute}
	sched := ratelimit.NewScheduler(cfg)
	defer sched.Drain(context.Background())
	result, err := sched.Submit(context.Background(), "client", ratelimit.PriorityLiveRefresh, func(ctx context.Context) error {
		events, err := source.Fetch(ctx, "BTCUSDT", from, to)
		if err != nil {
			return err
		}
		if len(events) != 2 || events[0].IntervalH != 8 || events[1].IntervalH != 4 {
			return fmt.Errorf("bad events: %+v", events)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || sched.Snapshot().NodeBudgetUsed != 3 {
		t.Fatalf("calls=%d budget=%d", calls.Load(), sched.Snapshot().NodeBudgetUsed)
	}
}

func TestBinanceFundingSourceFailsClosedAndChargesAttempts(t *testing.T) {
	data, checksum, pin := fundingFixture(t, "BTCUSDT-fundingRate-2024-01.csv", "calc_time,funding_interval_hours,last_funding_rate\n1704067200000,8,0.0001\n")
	tests := []struct {
		name     string
		budget   int
		pinned   bool
		checksum string
		api      string
		calls    int32
	}{
		{"missing pin", 3, false, checksum, `[]`, 0},
		{"checksum mismatch", 3, true, "bad", `[]`, 2},
		{"api discrepancy", 3, true, checksum, `[{"symbol":"BTCUSDT","fundingTime":1704067200000,"fundingRate":"0.0002"}]`, 3},
		{"budget exhausted", 2, true, checksum, `[]`, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch {
				case strings.HasSuffix(r.URL.Path, ".CHECKSUM"):
					fmt.Fprint(w, tt.checksum)
				case strings.HasSuffix(r.URL.Path, ".zip"):
					_, _ = w.Write(data)
				default:
					fmt.Fprint(w, tt.api)
				}
			}))
			defer server.Close()
			source := NewBinanceFundingSource(server.Client())
			source.archiveBase, source.apiBase = server.URL, server.URL
			source.pin = func(_, _, _ string) (string, bool) { return pin, tt.pinned }
			sched := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: tt.budget, PerClientQuotaPerMinute: tt.budget, MaxTrackedClients: 2, QueueCapacity: 2, WorkerCount: 1, RetryBaseDelay: time.Millisecond})
			defer sched.Drain(context.Background())
			result, err := sched.Submit(context.Background(), "client", ratelimit.PriorityLiveRefresh, func(ctx context.Context) error {
				_, err := source.Fetch(ctx, "BTCUSDT", 1704067200000, 1704124800000)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = <-result; err == nil {
				t.Fatal("expected refusal")
			}
			if calls.Load() != tt.calls {
				t.Fatalf("HTTP calls=%d want %d", calls.Load(), tt.calls)
			}
		})
	}
}
