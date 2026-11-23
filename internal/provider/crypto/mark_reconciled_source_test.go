package crypto

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
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
				if !tc.success && err == nil {
					return fmt.Errorf("accepted")
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
		})
	}
}
