package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/httpapi"
	"github.com/jedi108/market-data-gateway/internal/model"
	fakeprovider "github.com/jedi108/market-data-gateway/internal/provider/fake"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

// trendBase anchors the synthetic uptrend (2025-01-01 UTC); every test
// window lies after it, so prices stay positive and strictly rising.
const trendBase = 1735689600000

// trendingCandle makes the fake provider emit a steady uptrend with a
// sinusoidal range, so trend/volatility outputs are non-degenerate.
func trendingCandle(request model.CandleRequest, open int64) model.Candle {
	candle := fakeprovider.DefaultCandle(request, open)
	bar := float64((open - trendBase) / (candle.CloseTimeUTCMS - open))
	c := 100 + bar*0.01
	spread := 0.5 + 0.3*math.Abs(math.Sin(bar/7))
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 4, 64) }
	candle.Open, candle.High, candle.Low, candle.Close = f(c-0.005), f(c+spread), f(c-spread), f(c)
	return candle
}

// newFakeGateway runs the real gateway HTTP handler over the fake provider
// (the -provider=fake stack) on a loopback httptest server.
func newFakeGateway(t *testing.T) string {
	t.Helper()
	store, err := storage.Open(t.TempDir() + "/mcp.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	scheduler := ratelimit.NewScheduler(ratelimit.Config{NodeBudgetPerMinute: 1000, PerClientQuotaPerMinute: 1000, MaxTrackedClients: 100, QueueCapacity: 128, WorkerCount: 4, MaxRetries: 0, RetryBaseDelay: time.Millisecond})
	svc, err := service.New(cache.New(), store, fakeprovider.New().WithCandleFactory(trendingCandle), scheduler, service.Config{RequestTimeout: 5 * time.Second, MaxStaleAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := httpapi.NewGatewayHandlerWithIdentity("mcp-test", func() bool { return true }, svc, func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		if venue != "tbank" {
			return model.SeriesKey{}, apperror.New(apperror.CodeInvalidRequest, "unsupported venue", 0)
		}
		if symbol != "SBER" && symbol != "GAZP" {
			return model.SeriesKey{}, apperror.New(apperror.CodeUnknownSymbol, "unknown symbol", 0)
		}
		return model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "FIGI-" + symbol, Timeframe: timeframe, CandleType: "trade"}, nil
	}, 500, 20, 4, httpapi.WithNodeID("node-test"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = scheduler.Drain(ctx)
		store.Close()
	})
	return server.URL
}

func connect(t *testing.T, gatewayURL string) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	if _, err := newServer(newGateway(gatewayURL, ""), "test").Connect(ctx, serverTransport, nil); err != nil {
		t.Fatal(err)
	}
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "test"}, nil).Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

// call invokes a tool, requires success, and decodes structured output.
func call[T any](t *testing.T, session *mcp.ClientSession, name string, args map[string]any) T {
	t.Helper()
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: protocol error %v", name, err)
	}
	if result.IsError {
		t.Fatalf("%s: tool error %s", name, text(result))
	}
	data, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("%s: structured content %s: %v", name, data, err)
	}
	return out
}

func text(result *mcp.CallToolResult) string {
	var parts []string
	for _, content := range result.Content {
		if tc, ok := content.(*mcp.TextContent); ok {
			parts = append(parts, tc.Text)
		}
	}
	return strings.Join(parts, " ")
}

func TestToolsListDeclaresOutputSchemas(t *testing.T) {
	session := connect(t, newFakeGateway(t))
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"get_candles":     {"candles", "freshness", "incomplete"},
		"get_volatility":  {"atr", "atr_pct", "realized_vol", "realized_vol_annualized", "atr_percentile", "label"},
		"get_trend":       {"ema20", "ema50", "ema200", "ema20_slope_pct", "adx", "price_vs_ema200", "label", "strength"},
		"market_snapshot": {"items"},
		"gateway_status":  {"health", "ready", "venues", "timeframes"},
	}
	if len(tools.Tools) != len(want) {
		t.Fatalf("tools = %d, want %d", len(tools.Tools), len(want))
	}
	for _, tool := range tools.Tools {
		fields, ok := want[tool.Name]
		if !ok {
			t.Fatalf("unexpected tool %q", tool.Name)
		}
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Errorf("%s: missing readOnlyHint", tool.Name)
		}
		raw, _ := json.Marshal(tool.OutputSchema)
		var schema struct {
			Properties map[string]any `json:"properties"`
		}
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatal(err)
		}
		for _, field := range fields {
			if _, ok := schema.Properties[field]; !ok {
				t.Errorf("%s: output schema lacks %q: %s", tool.Name, field, raw)
			}
		}
	}
}

func TestToolsAgainstFakeGateway(t *testing.T) {
	session := connect(t, newFakeGateway(t))

	candles := call[candlesOut](t, session, "get_candles", map[string]any{"symbol": "SBER", "timeframe": "1m", "lookback": 50})
	if candles.Count != 50 || len(candles.Candles) != 50 || candles.Incomplete || candles.Freshness == "" || candles.Venue != "tbank" {
		t.Fatalf("get_candles lookback = %+v", candles)
	}
	window := call[candlesOut](t, session, "get_candles", map[string]any{"symbol": "SBER", "timeframe": "1h", "from": "2026-01-05T00:00:00Z", "to": "2026-01-25T00:00:00Z", "limit": 400})
	if window.Count != 400 || !window.Incomplete || window.Candles[0].Time != "2026-01-05T00:00:00Z" {
		t.Fatalf("get_candles window (480 bars over 3 chunks, cut to 400) = count %d incomplete %v first %+v", window.Count, window.Incomplete, window.Candles[0])
	}

	vol := call[volatilityOut](t, session, "get_volatility", map[string]any{"symbol": "SBER", "timeframe": "1m"})
	if vol.Window != 14 || vol.Lookback != 200 || vol.Bars != 215 || vol.ATR <= 0 || vol.ATRPct <= 0 || vol.RealizedVol <= 0 || vol.RealizedVolAnnualized <= vol.RealizedVol || vol.ATRPercentile < 0 || vol.ATRPercentile > 100 {
		t.Fatalf("get_volatility = %+v", vol)
	}
	if vol.Label != "low" && vol.Label != "normal" && vol.Label != "high" {
		t.Fatalf("volatility label %q", vol.Label)
	}

	trendResult := call[trendOut](t, session, "get_trend", map[string]any{"symbol": "SBER", "timeframe": "5m"})
	if trendResult.Bars != trendBars || trendResult.EMA200 == nil || trendResult.PriceVsEMA200 != "above" || trendResult.Label != "up" || trendResult.EMA20SlopePct <= 0 {
		t.Fatalf("get_trend on a steady uptrend = %+v", trendResult)
	}

	snapshot := call[snapshotOut](t, session, "market_snapshot", map[string]any{"symbols": []string{"SBER", "GAZP"}, "timeframe": "1h"})
	if len(snapshot.Items) != 2 || snapshot.Items[0].TrendLabel != "up" || snapshot.Items[0].VolatilityLabel == "" || snapshot.Items[1].LastClose == nil || *snapshot.Items[1].LastClose <= 0 || snapshot.Items[1].ChangePct == nil || snapshot.Items[1].Error != "" {
		t.Fatalf("market_snapshot = %+v", snapshot)
	}

	status := call[statusOut](t, session, "gateway_status", nil)
	if status.Health != "ok" || !status.Ready || status.Build != "mcp-test" || status.NodeID != "node-test" || len(status.Timeframes) != 5 {
		t.Fatalf("gateway_status = %+v", status)
	}

	prompt, err := session.GetPrompt(context.Background(), &mcp.GetPromptParams{Name: "market_brief", Arguments: map[string]string{"symbol": "SBER", "timeframe": "1h"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(prompt.Messages) != 1 || !strings.Contains(prompt.Messages[0].Content.(*mcp.TextContent).Text, "get_trend") {
		t.Fatalf("market_brief = %+v", prompt)
	}
	if _, err := session.GetPrompt(context.Background(), &mcp.GetPromptParams{Name: "market_brief", Arguments: map[string]string{"symbol": "SBER", "timeframe": "2h"}}); err == nil {
		t.Fatal("market_brief accepted a bad timeframe")
	}
}

func TestToolErrorsAreTyped(t *testing.T) {
	gatewayURL := newFakeGateway(t)
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	cases := []struct {
		name, gateway, tool string
		args                map[string]any
		code                string
	}{
		{"bad timeframe", gatewayURL, "get_volatility", map[string]any{"symbol": "SBER", "timeframe": "2h"}, "INVALID_REQUEST:"},
		{"unknown symbol", gatewayURL, "get_trend", map[string]any{"symbol": "NOPE", "timeframe": "1h"}, "UNKNOWN_SYMBOL:"},
		{"bad window", gatewayURL, "get_candles", map[string]any{"symbol": "SBER", "timeframe": "1m", "from": "yesterday"}, "INVALID_REQUEST:"},
		{"window unknown symbol", gatewayURL, "get_candles", map[string]any{"symbol": "NOPE", "timeframe": "1h", "from": "2026-01-05T00:00:00Z", "to": "2026-01-06T00:00:00Z"}, "UNKNOWN_SYMBOL:"},
		{"window too wide", gatewayURL, "get_candles", map[string]any{"symbol": "SBER", "timeframe": "1m", "from": "2026-01-01", "to": "2026-01-25"}, "INVALID_REQUEST:"},
		{"snapshot unknown symbol", gatewayURL, "market_snapshot", map[string]any{"symbols": []string{"SBER", "NOPE"}, "timeframe": "1h"}, "UNKNOWN_SYMBOL:"},
		{"bad periods_per_year", gatewayURL, "get_volatility", map[string]any{"symbol": "SBER", "timeframe": "1d", "periods_per_year": -1}, "INVALID_REQUEST:"},
		{"too many symbols", gatewayURL, "market_snapshot", map[string]any{"symbols": make([]string, 21), "timeframe": "1h"}, "INVALID_REQUEST:"},
		{"gateway down", down.URL, "get_volatility", map[string]any{"symbol": "SBER", "timeframe": "1m"}, "GATEWAY_UNAVAILABLE:"},
		{"gateway down status", down.URL, "gateway_status", nil, "GATEWAY_UNAVAILABLE:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			session := connect(t, tc.gateway)
			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.tool, Arguments: tc.args})
			if err != nil {
				t.Fatalf("protocol error instead of tool error: %v", err)
			}
			if !result.IsError || !strings.HasPrefix(text(result), tc.code) {
				t.Fatalf("isError=%v text=%q, want prefix %q", result.IsError, text(result), tc.code)
			}
		})
	}
}

func TestComputeTrendAndVolatilityLabels(t *testing.T) {
	n := 120
	high, low, down := make([]float64, n), make([]float64, n), make([]float64, n)
	for i := range down {
		down[i] = 200 - float64(i)
		high[i], low[i] = down[i]+0.5, down[i]-0.5
	}
	tr, err := computeTrend(high, low, down)
	if err != nil || tr.Label != "down" || tr.Strength != "strong" || tr.EMA200 != nil || tr.PriceVsEMA200 != "unknown" {
		t.Fatalf("steady downtrend = %+v, %v", tr, err)
	}
	if _, err := computeTrend(high[:10], low[:10], down[:10]); err == nil || !strings.HasPrefix(err.Error(), "INSUFFICIENT_DATA:") {
		t.Fatalf("short trend input err = %v", err)
	}
	// A spike in the last bar's range puts the current ATR at the top.
	spikeHigh := append([]float64(nil), high...)
	spikeHigh[n-1] += 50
	if v, _ := computeVolatility(spikeHigh, low, down, 14, 100, 60_000, 0); v.Label != "high" {
		t.Fatalf("range spike label = %q (percentile %v)", v.Label, v.ATRPercentile)
	}
	// periods_per_year: 0 keeps 365 calendar days, an explicit value overrides it.
	calendar, _ := computeVolatility(high, low, down, 14, 100, 86_400_000, 0)
	trading, _ := computeVolatility(high, low, down, 14, 100, 86_400_000, 252)
	if math.Abs(calendar.RealizedVolAnnualized-calendar.RealizedVol*math.Sqrt(365)) > 1e-12 || math.Abs(trading.RealizedVolAnnualized-trading.RealizedVol*math.Sqrt(252)) > 1e-12 {
		t.Fatalf("annualized calendar=%v trading=%v", calendar, trading)
	}
}

func TestHTTPTransportFailsClosed(t *testing.T) {
	for _, tc := range []struct{ listen, token, want string }{
		{"127.0.0.1:8090", "", "GATEWAY_MCP_TOKEN"},
		{"0.0.0.0:8090", "secret", "loopback or private"},
		{"8.8.8.8:8090", "secret", "loopback or private"},
		{"localhost:8090", "secret", "loopback or private"},
		{":8090", "secret", "loopback or private"},
	} {
		err := checkHTTP(tc.listen, tc.token)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("checkHTTP(%q) = %v, want error containing %q", tc.listen, err, tc.want)
		}
		if err != nil && tc.token != "" && strings.Contains(err.Error(), tc.token) {
			t.Errorf("error leaks the token: %v", err)
		}
	}
	for _, listen := range []string{"127.0.0.1:8090", "10.0.0.5:8090", "192.168.1.2:8090", "172.18.0.1:8090", "[::1]:8090"} {
		if err := checkHTTP(listen, "secret"); err != nil {
			t.Errorf("checkHTTP(%q) = %v, want ok", listen, err)
		}
	}
}

func TestHTTPTransportRequiresBearer(t *testing.T) {
	server := httptest.NewServer(httpHandler(newServer(newGateway(newFakeGateway(t), ""), "test"), "s3cret"))
	defer server.Close()
	for _, header := range []string{"", "Bearer wrong", "Basic s3cret", "Bearer "} {
		request, _ := http.NewRequest(http.MethodPost, server.URL+httpPath, strings.NewReader(`{}`))
		if header != "" {
			request.Header.Set("Authorization", header)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Errorf("Authorization %q -> %d, want 401", header, response.StatusCode)
		}
	}
	// The right token reaches MCP: a real client lists the tools over HTTP.
	client := mcp.NewClient(&mcp.Implementation{Name: "http-client", Version: "test"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL + httpPath, HTTPClient: &http.Client{Transport: bearerTransport{"s3cret"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil || len(tools.Tools) != 5 {
		t.Fatalf("tools over HTTP = %v, %v", tools, err)
	}
}

type bearerTransport struct{ token string }

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}
