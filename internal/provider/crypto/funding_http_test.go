package crypto

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/httpapi"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

func TestFundingHTTPPreservesReconciledArchiveIntervalsAndAvailability(t *testing.T) {
	data, checksum, pin := fundingFixture(t, "BTCUSDT-fundingRate-2024-01.csv", "calc_time,funding_interval_hours,last_funding_rate\n1704067200000,8,0.0001\n1704096000000,4,-0.0002\n")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer upstream.Close()
	source := NewBinanceFundingSource(upstream.Client())
	source.archiveBase, source.apiBase = upstream.URL, upstream.URL
	source.pin = func(_, _, _ string) (string, bool) { return pin, true }
	store, err := storage.Open(t.TempDir() + "/cache.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 100, PerClientQuotaPerMinute: 100, MaxTrackedClients: 10, QueueCapacity: 10, WorkerCount: 1, RetryBaseDelay: time.Millisecond})
	defer scheduler.Drain(context.Background())
	svc, err := service.New(cache.New(), store, &DiagnosticProvider{funding: source}, scheduler, service.Config{RequestTimeout: time.Second, MaxStaleAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		if venue != "binance" || symbol != "BTC/USDT:USDT" || timeframe != "8h" {
			return model.SeriesKey{}, fmt.Errorf("wrong identity")
		}
		return model.SeriesKey{Venue: venue, MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: timeframe, CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true}, nil
	}
	h, err := httpapi.NewGatewayHandlerWithIdentity("test", func() bool { return true }, svc, resolve, 10, 10, 1, httpapi.WithFundingResolver(resolve))
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/candles?venue=binance&symbol=BTC%2FUSDT%3AUSDT&timeframe=8h&from_utc_ms=1704067200000&to_utc_ms=1704124800000&limit=2&include_incomplete=false&candle_type=funding&completeness=reconciled&diagnostic_only=true"
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		var body struct {
			FundingEvents []model.FundingEvent `json:"funding_events"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if len(body.FundingEvents) != 2 {
			t.Fatalf("events: %+v", body.FundingEvents)
		}
		for n, want := range []struct {
			ts       int64
			interval int
		}{{1704067200000, 8}, {1704096000000, 4}} {
			e := body.FundingEvents[n]
			if e.Ts != want.ts || e.IntervalH != want.interval || e.AvailableAt != want.ts || e.Availability != "historical_payment_assumption" {
				t.Fatalf("event %d: %+v", n, e)
			}
		}
	}
}
