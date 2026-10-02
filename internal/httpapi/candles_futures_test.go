package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/observability"
	tbankprovider "github.com/jedi108/market-data-gateway/internal/provider/tbank"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

// Synthetic offline futures instruments only (F20): no real physical GOLD UID
// exists offline, so the acceptance contract is pinned with synthetic UIDs.
func futuresRegistryFixture(t *testing.T) *tbankprovider.Registry {
	t.Helper()
	registry, err := tbankprovider.NewRegistry([]tbankprovider.Instrument{
		{CanonicalSymbol: "SBER", ProviderInstrumentID: "BBG004730N88", MarketType: "shares", Aliases: []string{"SBER/RUB"}},
		{CanonicalSymbol: "GOLD-26.12", ProviderInstrumentID: "synthetic-uid-gold-2612", MarketType: "futures", Aliases: []string{"GOLD"}, ExpirationUTCMS: 1797043200000},
		{CanonicalSymbol: "GOLD-27.03", ProviderInstrumentID: "synthetic-uid-gold-2703", MarketType: "futures", ExpirationUTCMS: 1809609600000},
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

type recordingProvider struct {
	mu       sync.Mutex
	requests []model.SeriesKey
}

func (p *recordingProvider) FetchCandles(ctx context.Context, r model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	p.mu.Lock()
	p.requests = append(p.requests, r.Series)
	p.mu.Unlock()
	var candles []model.Candle
	for open := r.FromUTCMS; open < r.ToUTCMS; open += 60_000 {
		candles = append(candles, model.Candle{OpenTimeUTCMS: open, CloseTimeUTCMS: open + 60_000, Open: "24510", High: "24600", Low: "24480", Close: "24550", Volume: "7", IsClosed: true})
	}
	return candles, model.ProviderMetadata{Status: "fake"}, nil
}

func (p *recordingProvider) series() []model.SeriesKey {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]model.SeriesKey(nil), p.requests...)
}

type futuresFixture struct {
	server   *httptest.Server
	store    *storage.Store
	registry *tbankprovider.Registry
	provider *recordingProvider
	recorder *observability.Recorder
}

func newFuturesFixture(t *testing.T, path string, registry *tbankprovider.Registry) *futuresFixture {
	t.Helper()
	store, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 128, WorkerCount: 4, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	provider := &recordingProvider{}
	recorder := observability.NewRecorder()
	recorder.RecordRegistryState("tbank", tbankprovider.RegistrySchemaVersion, registry.Refreshes(), registryInstruments(registry))
	svc, err := service.New(cache.New(), store, provider, scheduler, service.Config{RequestTimeout: 2 * time.Second, MaxStaleAge: time.Hour, Recorder: recorder})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewGatewayHandlerWithIdentity("futures-test", func() bool { return true }, svc, func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		if venue != "tbank" {
			return model.SeriesKey{}, fmt.Errorf("unsupported venue")
		}
		instrument, err := registry.Resolve(symbol)
		if err != nil {
			return model.SeriesKey{}, err
		}
		return model.SeriesKey{Venue: "tbank", MarketType: instrument.MarketType, ProviderInstrumentID: instrument.ProviderInstrumentID, Timeframe: timeframe, CandleType: "trade"}, nil
	}, 500, 64, 8, WithSeriesDescriptors(registry.Describe))
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
	return &futuresFixture{server: server, store: store, registry: registry, provider: provider, recorder: recorder}
}

func registryInstruments(registry *tbankprovider.Registry) []observability.RegistryInstrument {
	instruments := registry.Instruments()
	out := make([]observability.RegistryInstrument, 0, len(instruments))
	for _, instrument := range instruments {
		out = append(out, observability.RegistryInstrument{MarketType: instrument.MarketType, InstrumentType: instrument.InstrumentType, Symbol: instrument.CanonicalSymbol, ExpirationUTCMS: instrument.ExpirationUTCMS})
	}
	return out
}

func (f *futuresFixture) candlesURL(symbol string, from, to int64) string {
	return fmt.Sprintf("%s/v1/candles?venue=tbank&symbol=%s&timeframe=1m&from_utc_ms=%d&to_utc_ms=%d&limit=500&include_incomplete=false", f.server.URL, symbol, from, to)
}

type candleResponse struct {
	SchemaVersion int                       `json:"schema_version"`
	Series        model.SeriesKey           `json:"series"`
	Candles       []model.Candle            `json:"candles"`
	Instrument    *model.InstrumentMetadata `json:"instrument"`
}

func (f *futuresFixture) getCandles(t *testing.T, url string) (int, candleResponse, string) {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var decoded candleResponse
	_ = json.Unmarshal(body, &decoded)
	return response.StatusCode, decoded, string(body)
}

// TestFuturesResponseCarriesPhysicalIdentityAndUnits is the F20 acceptance
// contract: a logical-alias request resolves to the physical contract UID, the
// upstream provider is called with that UID, and the response carries explicit
// instrument identity/units so downstream never reconstructs the physical
// contract from the ticker.
func TestFuturesResponseCarriesPhysicalIdentityAndUnits(t *testing.T) {
	f := newFuturesFixture(t, t.TempDir()+"/futures.sqlite", futuresRegistryFixture(t))
	status, decoded, body := f.getCandles(t, f.candlesURL("GOLD", 0, 120_000))
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%s", status, body)
	}
	if decoded.Series.ProviderInstrumentID != "synthetic-uid-gold-2612" {
		t.Fatalf("series identity must be the physical UID: %+v", decoded.Series)
	}
	if decoded.Series.MarketType != "futures" {
		t.Fatalf("series market type: %+v", decoded.Series)
	}
	if decoded.Instrument == nil {
		t.Fatalf("response missing instrument metadata: %s", body)
	}
	if decoded.Instrument.InstrumentType != model.InstrumentTypeFuture || decoded.Instrument.PriceUnit != model.PriceUnitPoints || decoded.Instrument.VolumeUnit != model.VolumeUnitLots || decoded.Instrument.ExpirationUTCMS != 1797043200000 || decoded.Instrument.CanonicalSymbol != "GOLD-26.12" {
		t.Fatalf("futures instrument metadata: %+v", decoded.Instrument)
	}
	if decoded.Instrument.Currency != "" {
		t.Fatalf("futures price domain is points, currency must be empty: %+v", decoded.Instrument)
	}
	series := f.provider.series()
	if len(series) != 1 || series[0].ProviderInstrumentID != "synthetic-uid-gold-2612" {
		t.Fatalf("provider saw series %v, want exactly the physical December UID", series)
	}
}

// TestShareResponseGainsMetadataWithoutIdentityChange pins share back-compat:
// the same share request now carries instrument metadata, but its series key
// and provider FIGI identity are byte-identical to the pre-futures contract.
func TestShareResponseGainsMetadataWithoutIdentityChange(t *testing.T) {
	f := newFuturesFixture(t, t.TempDir()+"/shares.sqlite", futuresRegistryFixture(t))
	status, decoded, body := f.getCandles(t, f.candlesURL("SBER", 0, 60_000))
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%s", status, body)
	}
	if decoded.Series.ProviderInstrumentID != "BBG004730N88" || decoded.Series.MarketType != "shares" {
		t.Fatalf("share series identity changed: %+v", decoded.Series)
	}
	if decoded.Instrument == nil || decoded.Instrument.InstrumentType != model.InstrumentTypeShare || decoded.Instrument.PriceUnit != model.PriceUnitCurrency || decoded.Instrument.VolumeUnit != model.VolumeUnitLots {
		t.Fatalf("share instrument metadata: %+v body=%s", decoded.Instrument, body)
	}
	if decoded.Instrument.ExpirationUTCMS != 0 {
		t.Fatalf("share must not carry an expiration: %+v", decoded.Instrument)
	}
}

// TestUnknownFuturesSymbolFailsExplicitly proves the no-silent-fallback rule:
// a futures ticker outside the registry fails with the typed unknown-symbol
// error before any provider call — never an empty 200 candles response.
func TestUnknownFuturesSymbolFailsExplicitly(t *testing.T) {
	f := newFuturesFixture(t, t.TempDir()+"/unknown.sqlite", futuresRegistryFixture(t))
	response, err := http.Get(f.candlesURL("SILVER", 0, 60_000))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("unknown symbol status=%d body=%s", response.StatusCode, body)
	}
	if string(body) == "" || !json.Valid(body) {
		t.Fatalf("error response must be an explicit envelope: %s", body)
	}
	var envelope struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("error envelope decode: %v body=%s", err, body)
	}
	if envelope.Error != "UNKNOWN_SYMBOL" || envelope.Message == "" {
		t.Fatalf("typed error envelope: %+v body=%s", envelope, body)
	}
	if calls := f.provider.series(); len(calls) != 0 {
		t.Fatalf("unknown symbol must not reach the provider, saw %v", calls)
	}
}

// TestFuturesTargetSwitchDoesNotRewriteOldContractHistory is the F20 roll
// acceptance: after the registry rolls the logical alias to the next expiry,
// new alias traffic hits the new physical UID while the old contract's cached
// history stays intact under its own identity.
func TestFuturesTargetSwitchDoesNotRewriteOldContractHistory(t *testing.T) {
	path := t.TempDir() + "/roll.sqlite"
	registry := futuresRegistryFixture(t)
	f := newFuturesFixture(t, path, registry)
	if status, _, body := f.getCandles(t, f.candlesURL("GOLD", 0, 120_000)); status != http.StatusOK {
		t.Fatalf("december fetch status=%d body=%s", status, body)
	}
	// Roll: the alias moves to March; December stays listed without it.
	if err := registry.Refresh([]tbankprovider.Instrument{
		{CanonicalSymbol: "SBER", ProviderInstrumentID: "BBG004730N88", MarketType: "shares", Aliases: []string{"SBER/RUB"}},
		{CanonicalSymbol: "GOLD-26.12", ProviderInstrumentID: "synthetic-uid-gold-2612", MarketType: "futures", ExpirationUTCMS: 1797043200000},
		{CanonicalSymbol: "GOLD-27.03", ProviderInstrumentID: "synthetic-uid-gold-2703", MarketType: "futures", Aliases: []string{"GOLD"}, ExpirationUTCMS: 1809609600000},
	}); err != nil {
		t.Fatal(err)
	}
	status, decoded, body := f.getCandles(t, f.candlesURL("GOLD", 0, 120_000))
	if status != http.StatusOK {
		t.Fatalf("march fetch status=%d body=%s", status, body)
	}
	if decoded.Series.ProviderInstrumentID != "synthetic-uid-gold-2703" || decoded.Instrument == nil || decoded.Instrument.ExpirationUTCMS != 1809609600000 {
		t.Fatalf("alias did not roll to March: series=%+v instrument=%+v", decoded.Series, decoded.Instrument)
	}
	// Old contract history is still served under its physical UID identity.
	status, old, body := f.getCandles(t, f.candlesURL("synthetic-uid-gold-2612", 0, 120_000))
	if status != http.StatusOK {
		t.Fatalf("old UID fetch status=%d body=%s", status, body)
	}
	if old.Series.ProviderInstrumentID != "synthetic-uid-gold-2612" || len(old.Candles) != 2 || old.Instrument.CanonicalSymbol != "GOLD-26.12" {
		t.Fatalf("old contract history compromised: series=%+v candles=%d instrument=%+v", old.Series, len(old.Candles), old.Instrument)
	}
	// Provider traffic: December once, March once — the old series never got
	// re-fetched through the alias, and the new series is a distinct fetch.
	series := f.provider.series()
	perUID := map[string]int{}
	for _, key := range series {
		perUID[key.ProviderInstrumentID]++
	}
	if perUID["synthetic-uid-gold-2612"] != 1 || perUID["synthetic-uid-gold-2703"] != 1 {
		t.Fatalf("provider traffic after roll: %v", perUID)
	}
}

// TestFuturesIdentityMappingSurvivesRestart proves restart recovery end to
// end: after a roll and a cold start on the same SQLite path, the old
// contract's series is served as a persistent hit under its physical UID and
// the new alias target resolves without re-fetching the old history.
func TestFuturesIdentityMappingSurvivesRestart(t *testing.T) {
	path := t.TempDir() + "/restart.sqlite"
	registry := futuresRegistryFixture(t)
	first := newFuturesFixture(t, path, registry)
	if status, _, body := first.getCandles(t, first.candlesURL("GOLD", 0, 120_000)); status != http.StatusOK {
		t.Fatalf("warm status=%d body=%s", status, body)
	}
	rolled := []tbankprovider.Instrument{
		{CanonicalSymbol: "GOLD-26.12", ProviderInstrumentID: "synthetic-uid-gold-2612", MarketType: "futures", ExpirationUTCMS: 1797043200000},
		{CanonicalSymbol: "GOLD-27.03", ProviderInstrumentID: "synthetic-uid-gold-2703", MarketType: "futures", Aliases: []string{"GOLD"}, ExpirationUTCMS: 1809609600000},
	}
	if err := registry.Refresh(rolled); err != nil {
		t.Fatal(err)
	}
	// Cold start: a fresh registry built from the persisted mapping state.
	restartedRegistry, err := tbankprovider.NewRegistry(rolled)
	if err != nil {
		t.Fatal(err)
	}
	second := newFuturesFixture(t, path, restartedRegistry)
	status, decoded, body := second.getCandles(t, second.candlesURL("synthetic-uid-gold-2612", 0, 120_000))
	if status != http.StatusOK {
		t.Fatalf("restart old UID status=%d body=%s", status, body)
	}
	if len(decoded.Candles) != 2 {
		t.Fatalf("restart lost old contract candles: %d", len(decoded.Candles))
	}
	if calls := second.provider.series(); len(calls) != 0 {
		t.Fatalf("restart must serve the old series from persistent cache, saw provider calls %v", calls)
	}
}

// TestBatchCarriesFuturesInstrumentMetadata pins the batch envelope: per-item
// instrument metadata is present for futures items, omitted never invents.
func TestBatchCarriesFuturesInstrumentMetadata(t *testing.T) {
	f := newFuturesFixture(t, t.TempDir()+"/batch.sqlite", futuresRegistryFixture(t))
	request, err := json.Marshal(map[string]any{"items": []map[string]any{{
		"venue": "tbank", "symbol": "GOLD", "timeframe": "1m", "from_utc_ms": 0, "to_utc_ms": 60000, "limit": 10, "include_incomplete": false,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(f.server.URL+"/v1/candles/batch", "application/json", bytes.NewReader(request))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("batch status=%d body=%s", response.StatusCode, body)
	}
	var envelope struct {
		Items []struct {
			Series     model.SeriesKey           `json:"series"`
			Candles    []model.Candle            `json:"candles"`
			Instrument *model.InstrumentMetadata `json:"instrument"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Items) != 1 {
		t.Fatalf("batch items=%d body=%s", len(envelope.Items), body)
	}
	item := envelope.Items[0]
	if item.Series.ProviderInstrumentID != "synthetic-uid-gold-2612" || item.Instrument == nil || item.Instrument.InstrumentType != model.InstrumentTypeFuture || item.Instrument.PriceUnit != model.PriceUnitPoints {
		t.Fatalf("batch futures item: series=%+v instrument=%+v", item.Series, item.Instrument)
	}
	if len(item.Candles) != 1 {
		t.Fatalf("batch candles=%d body=%s", len(item.Candles), body)
	}
}

// TestMetricsExposeFuturesRegistryAndSourceFamilies checks the F20 metric
// families after futures traffic: registry state, cache source split by
// market, and fetch-provenance source age.
func TestMetricsExposeFuturesRegistryAndSourceFamilies(t *testing.T) {
	f := newFuturesFixture(t, t.TempDir()+"/metrics.sqlite", futuresRegistryFixture(t))
	if status, _, body := f.getCandles(t, f.candlesURL("GOLD", 0, 60_000)); status != http.StatusOK {
		t.Fatalf("fetch status=%d body=%s", status, body)
	}
	response, err := http.Get(f.server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	text := string(body)
	for _, want := range []string{
		"# TYPE gateway_registry_instruments gauge",
		`gateway_registry_instruments{venue="tbank",market_type="futures",instrument_type="future",symbol="GOLD-26-12"} 1`,
		"# TYPE gateway_registry_active_contract_expiry_epoch_seconds gauge",
		`gateway_registry_active_contract_expiry_epoch_seconds{venue="tbank",market_type="futures",instrument_type="future",symbol="GOLD-26-12"} 1797043200`,
		"# TYPE gateway_registry_refreshes_total counter",
		"# TYPE gateway_registry_schema_version gauge",
		`gateway_registry_schema_version 1`,
		"# TYPE gateway_series_cache_source_total counter",
		`gateway_series_cache_source_total{venue="tbank",market_type="futures",source="refreshed"} 1`,
		"# TYPE gateway_series_source_age_seconds gauge",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
}
