package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/jedi108/market-data-gateway/internal/indicators"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// Label thresholds. They are quoted verbatim in the tool descriptions so an
// agent can see exactly how every label is derived.
const (
	volLowPercentile  = 25.0 // ATR percentile below this -> "low"
	volHighPercentile = 75.0 // ATR percentile above this -> "high"
	adxTrendMin       = 20.0 // ADX below this -> trend "flat"
	adxStrong         = 25.0 // ADX at or above this -> strength "strong"
	slopeBars         = 5    // EMA20 slope is measured over this many bars
	trendBars         = 300  // bars fetched by get_trend (EMA200 warm-up)
	minTrendBars      = 60   // fewer closed bars -> INSUFFICIENT_DATA
	maxCandles        = 1000 // get_candles limit / lookback upper bound
	maxWindowBars     = 3000 // widest from/to window, in timeframe steps
	maxSnapshotItems  = 20   // market_snapshot symbols per call
	defaultVenue      = "tbank"
	// shortcut: a lookback of N bars requests a calendar window of
	// gapFactor*N steps and keeps the last N closed bars, so session gaps
	// (nights, weekends) of session-traded venues are absorbed; very long
	// holidays can still yield fewer bars (reported as incomplete).
	gapFactor = 3
)

// timeframes are the gateway timeframes and their step in milliseconds.
var timeframes = map[string]int64{"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "1d": 86_400_000}

// Error codes of this server. Gateway error codes (UNKNOWN_SYMBOL,
// INVALID_REQUEST, GATEWAY_OVERLOADED, ...) are passed through unchanged.
const (
	codeInvalidRequest     = "INVALID_REQUEST"
	codeGatewayUnavailable = "GATEWAY_UNAVAILABLE"
	codeInsufficientData   = "INSUFFICIENT_DATA"
	codeIncompleteCoverage = "INCOMPLETE_COVERAGE"
)

// toolError becomes an MCP tool error (isError: true) whose text starts with
// the typed code: "CODE: message".
type toolError struct{ code, message string }

func (e *toolError) Error() string { return e.code + ": " + e.message }

func invalid(format string, args ...any) error {
	return &toolError{codeInvalidRequest, fmt.Sprintf(format, args...)}
}

// gateway is a thin read-only client of the gateway client API.
type gateway struct {
	baseURL string
	token   string // optional gateway client credential (MARKET_DATA_GATEWAY_TOKEN); never logged
	client  *http.Client
	now     func() time.Time
}

func newGateway(baseURL, token string) *gateway {
	return &gateway{baseURL: strings.TrimRight(baseURL, "/"), token: token, client: &http.Client{Timeout: 30 * time.Second}, now: time.Now}
}

// call performs one request and decodes a 2xx JSON body into out. Transport
// failures map to GATEWAY_UNAVAILABLE; non-2xx answers to the gateway's typed
// error envelope code.
func (g *gateway) call(ctx context.Context, method, path string, body, out any) error {
	var reader *bytes.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	} else {
		reader = bytes.NewReader(nil)
	}
	request, err := http.NewRequestWithContext(ctx, method, g.baseURL+path, reader)
	if err != nil {
		return &toolError{codeGatewayUnavailable, "invalid gateway URL"}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Client-ID", "gateway-mcp")
	if g.token != "" {
		request.Header.Set("Authorization", "Bearer "+g.token)
	}
	response, err := g.client.Do(request)
	if err != nil {
		return &toolError{codeGatewayUnavailable, "gateway is not reachable"}
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		var envelope struct{ Error, Message string }
		if json.NewDecoder(response.Body).Decode(&envelope) != nil || envelope.Error == "" {
			return &toolError{codeGatewayUnavailable, "gateway answered HTTP " + strconv.Itoa(response.StatusCode)}
		}
		return &toolError{envelope.Error, envelope.Message}
	}
	if err := json.NewDecoder(response.Body).Decode(out); err != nil {
		return &toolError{codeGatewayUnavailable, "gateway returned an unreadable body"}
	}
	return nil
}

type batchItem struct {
	Venue             string `json:"venue"`
	Symbol            string `json:"symbol"`
	Timeframe         string `json:"timeframe"`
	FromUTCMS         int64  `json:"from_utc_ms"`
	ToUTCMS           int64  `json:"to_utc_ms"`
	Limit             int    `json:"limit"`
	IncludeIncomplete bool   `json:"include_incomplete"`
}

type batchResult struct {
	Candles   []model.Candle `json:"candles"`
	Freshness string         `json:"freshness"`
	Error     string         `json:"error"`
	Message   string         `json:"message"`
}

// batch is used for distinct series only (market_snapshot).
func (g *gateway) batch(ctx context.Context, items []batchItem) ([]batchResult, error) {
	var envelope struct {
		Items []batchResult `json:"items"`
	}
	if err := g.call(ctx, http.MethodPost, "/v1/candles/batch", map[string]any{"items": items}, &envelope); err != nil {
		return nil, err
	}
	if len(envelope.Items) != len(items) {
		return nil, &toolError{codeGatewayUnavailable, "gateway batch answer does not match the request"}
	}
	return envelope.Items, nil
}

// chunkBars is the largest single gateway window: the gateway bounds 1h
// windows to 7 days and the example configs cap limit at 500.
func chunkBars(timeframe string) int64 {
	if timeframe == "1h" {
		return 168
	}
	return 500
}

// fetch returns the closed candles of [from, to) as sequential gateway
// windows of at most chunkBars bars. Chunks are not sent as one batch: the
// gateway coalesces concurrent requests per series (singleflight), so
// different windows of one series must not run concurrently. An empty chunk
// (INCOMPLETE_COVERAGE: nights, weekends) is a gap, not an error, unless the
// whole window is empty.
func (g *gateway) fetch(ctx context.Context, venue, symbol, timeframe string, from, to int64) ([]model.Candle, string, error) {
	step := timeframes[timeframe]
	if (to-from)/step > maxWindowBars {
		return nil, "", invalid("window is wider than %d %s bars", maxWindowBars, timeframe)
	}
	if to <= from {
		return nil, "", invalid("window is empty")
	}
	var candles []model.Candle
	freshness := ""
	var gap error
	for start := from; start < to; start += chunkBars(timeframe) * step {
		end := min(start+chunkBars(timeframe)*step, to)
		query := url.Values{"venue": {venue}, "symbol": {symbol}, "timeframe": {timeframe}, "from_utc_ms": {strconv.FormatInt(start, 10)}, "to_utc_ms": {strconv.FormatInt(end, 10)}, "limit": {strconv.FormatInt(max((end-start)/step, 1), 10)}, "include_incomplete": {"false"}}
		var result batchResult
		err := g.call(ctx, http.MethodGet, "/v1/candles?"+query.Encode(), nil, &result)
		var typed *toolError
		if errors.As(err, &typed) && typed.code == codeIncompleteCoverage {
			gap = err
			continue
		}
		if err != nil {
			return nil, "", err
		}
		candles = append(candles, result.Candles...)
		freshness = result.Freshness
	}
	if len(candles) == 0 && gap != nil {
		return nil, "", gap
	}
	return candles, freshness, nil
}

// lastBars returns up to n most recent closed bars.
func (g *gateway) lastBars(ctx context.Context, venue, symbol, timeframe string, n int) ([]model.Candle, string, error) {
	step := timeframes[timeframe]
	to := g.now().UnixMilli() / step * step
	candles, freshness, err := g.fetch(ctx, venue, symbol, timeframe, to-int64(gapFactor*n)*step, to)
	if len(candles) > n {
		candles = candles[len(candles)-n:]
	}
	return candles, freshness, err
}

func checkSeries(venue *string, symbol, timeframe string) error {
	if *venue == "" {
		*venue = defaultVenue
	}
	if strings.TrimSpace(symbol) == "" {
		return invalid("symbol is required")
	}
	if _, ok := timeframes[timeframe]; !ok {
		return invalid("timeframe %q is not one of 1m, 5m, 15m, 1h, 1d", timeframe)
	}
	return nil
}

// ohlc converts gateway decimal strings into float series.
func ohlc(candles []model.Candle) (high, low, close []float64, err error) {
	for _, c := range candles {
		h, err1 := strconv.ParseFloat(c.High, 64)
		l, err2 := strconv.ParseFloat(c.Low, 64)
		cl, err3 := strconv.ParseFloat(c.Close, 64)
		if err := errors.Join(err1, err2, err3); err != nil {
			return nil, nil, nil, &toolError{codeGatewayUnavailable, "gateway returned a non-numeric candle"}
		}
		high, low, close = append(high, h), append(low, l), append(close, cl)
	}
	return high, low, close, nil
}

// --- get_candles ---

type candlesIn struct {
	Venue     string `json:"venue,omitempty" jsonschema:"gateway venue: tbank (default), binance or bybit"`
	Symbol    string `json:"symbol" jsonschema:"instrument symbol, e.g. SBER or a futures alias"`
	Timeframe string `json:"timeframe" jsonschema:"one of 1m, 5m, 15m, 1h, 1d"`
	Lookback  int    `json:"lookback,omitempty" jsonschema:"most recent closed bars, 1..1000 (default 100); ignored when from/to are set"`
	From      string `json:"from,omitempty" jsonschema:"ISO-8601 window start, inclusive (RFC 3339 or YYYY-MM-DD, UTC)"`
	To        string `json:"to,omitempty" jsonschema:"ISO-8601 window end, exclusive (default now)"`
	Limit     int    `json:"limit,omitempty" jsonschema:"max bars returned in from/to mode, 1..1000 (default 1000)"`
}

type candleRow struct {
	Time   string `json:"time" jsonschema:"bar open time, RFC 3339 UTC"`
	Open   string `json:"open"`
	High   string `json:"high"`
	Low    string `json:"low"`
	Close  string `json:"close"`
	Volume string `json:"volume"`
	Closed bool   `json:"closed"`
}

type candlesOut struct {
	Venue      string      `json:"venue"`
	Symbol     string      `json:"symbol"`
	Timeframe  string      `json:"timeframe"`
	Count      int         `json:"count"`
	Freshness  string      `json:"freshness" jsonschema:"gateway freshness of the most recent non-empty chunk"`
	Incomplete bool        `json:"incomplete" jsonschema:"true when fewer bars than requested were returned (lookback mode) or the window was cut by limit (from/to mode)"`
	Candles    []candleRow `json:"candles"`
}

func parseTime(value string) (int64, error) {
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, value); err == nil {
			return t.UnixMilli(), nil
		}
	}
	return 0, invalid("time %q is not ISO-8601", value)
}

func (g *gateway) getCandles(ctx context.Context, _ *mcp.CallToolRequest, in candlesIn) (*mcp.CallToolResult, candlesOut, error) {
	out := candlesOut{}
	if err := checkSeries(&in.Venue, in.Symbol, in.Timeframe); err != nil {
		return nil, out, err
	}
	step := timeframes[in.Timeframe]
	var candles []model.Candle
	var freshness string
	var err error
	incomplete := false
	if in.From == "" && in.To == "" {
		if in.Lookback == 0 {
			in.Lookback = 100
		}
		if in.Lookback < 1 || in.Lookback > maxCandles {
			return nil, out, invalid("lookback must be 1..%d", maxCandles)
		}
		candles, freshness, err = g.lastBars(ctx, in.Venue, in.Symbol, in.Timeframe, in.Lookback)
		incomplete = len(candles) < in.Lookback
	} else {
		if in.From == "" {
			return nil, out, invalid("from is required with to")
		}
		if in.Limit == 0 {
			in.Limit = maxCandles
		}
		if in.Limit < 1 || in.Limit > maxCandles {
			return nil, out, invalid("limit must be 1..%d", maxCandles)
		}
		var from, to int64
		if from, err = parseTime(in.From); err != nil {
			return nil, out, err
		}
		to = g.now().UnixMilli() / step * step
		if in.To != "" {
			if to, err = parseTime(in.To); err != nil {
				return nil, out, err
			}
		}
		// The gateway serves windows aligned to the timeframe grid.
		from, to = (from+step-1)/step*step, to/step*step
		if from < 0 || to <= from {
			return nil, out, invalid("from must be before to (after alignment to %s)", in.Timeframe)
		}
		candles, freshness, err = g.fetch(ctx, in.Venue, in.Symbol, in.Timeframe, from, to)
		if len(candles) > in.Limit {
			candles, incomplete = candles[:in.Limit], true
		}
	}
	if err != nil {
		return nil, out, err
	}
	out = candlesOut{Venue: in.Venue, Symbol: in.Symbol, Timeframe: in.Timeframe, Count: len(candles), Freshness: freshness, Incomplete: incomplete, Candles: make([]candleRow, 0, len(candles))}
	for _, c := range candles {
		out.Candles = append(out.Candles, candleRow{Time: time.UnixMilli(c.OpenTimeUTCMS).UTC().Format(time.RFC3339), Open: c.Open, High: c.High, Low: c.Low, Close: c.Close, Volume: c.Volume, Closed: c.IsClosed})
	}
	return nil, out, nil
}

// --- get_volatility ---

type volatilityIn struct {
	Venue     string `json:"venue,omitempty" jsonschema:"gateway venue: tbank (default), binance or bybit"`
	Symbol    string `json:"symbol" jsonschema:"instrument symbol"`
	Timeframe string `json:"timeframe" jsonschema:"one of 1m, 5m, 15m, 1h, 1d"`
	Window    int    `json:"window,omitempty" jsonschema:"ATR period, 2..100 (default 14)"`
	Lookback  int    `json:"lookback,omitempty" jsonschema:"bars for the ATR percentile and realized vol, 10..800 (default 200)"`
}

type volatility struct {
	ATR                   float64 `json:"atr"`
	ATRPct                float64 `json:"atr_pct" jsonschema:"ATR as % of the last close"`
	RealizedVol           float64 `json:"realized_vol" jsonschema:"sample stdev of log returns per bar over lookback"`
	RealizedVolAnnualized float64 `json:"realized_vol_annualized" jsonschema:"realized_vol * sqrt(365 days / timeframe)"`
	ATRPercentile         float64 `json:"atr_percentile" jsonschema:"percentile rank (0..100) of the current ATR among the lookback ATR values"`
	Label                 string  `json:"label" jsonschema:"low, normal or high"`
}

type volatilityOut struct {
	Venue     string  `json:"venue"`
	Symbol    string  `json:"symbol"`
	Timeframe string  `json:"timeframe"`
	Window    int     `json:"window"`
	Lookback  int     `json:"lookback"`
	Bars      int     `json:"bars" jsonschema:"closed bars used"`
	LastClose float64 `json:"last_close"`
	Freshness string  `json:"freshness"`
	volatility
}

// computeVolatility derives the volatility block; percentile rank counts ties
// as half so a constant ATR series sits at 50.
func computeVolatility(high, low, close []float64, window, lookback int, step int64) (volatility, error) {
	n := len(close)
	if n < window+2 {
		return volatility{}, &toolError{codeInsufficientData, fmt.Sprintf("%d closed bars, need at least %d", n, window+2)}
	}
	atr := indicators.ATR(high, low, close, window)
	current := atr[n-1]
	series := atr[max(window, n-lookback):]
	below, equal := 0.0, 0.0
	for _, v := range series {
		if v < current {
			below++
		} else if v == current {
			equal++
		}
	}
	percentile := 100 * (below + equal/2) / float64(len(series))
	rv := indicators.RealizedVol(close[max(0, n-lookback-1):])
	if math.IsNaN(rv) || math.IsInf(rv, 0) || close[n-1] == 0 {
		return volatility{}, &toolError{codeInsufficientData, "not enough closes for realized volatility"}
	}
	label := "normal"
	if percentile < volLowPercentile {
		label = "low"
	} else if percentile > volHighPercentile {
		label = "high"
	}
	periodsPerYear := float64(365*24*time.Hour/time.Millisecond) / float64(step)
	return volatility{ATR: current, ATRPct: 100 * current / close[n-1], RealizedVol: rv, RealizedVolAnnualized: rv * math.Sqrt(periodsPerYear), ATRPercentile: percentile, Label: label}, nil
}

func (g *gateway) getVolatility(ctx context.Context, _ *mcp.CallToolRequest, in volatilityIn) (*mcp.CallToolResult, volatilityOut, error) {
	out := volatilityOut{}
	if err := checkSeries(&in.Venue, in.Symbol, in.Timeframe); err != nil {
		return nil, out, err
	}
	if in.Window == 0 {
		in.Window = 14
	}
	if in.Lookback == 0 {
		in.Lookback = 200
	}
	if in.Window < 2 || in.Window > 100 || in.Lookback < 10 || in.Lookback > 800 {
		return nil, out, invalid("window must be 2..100 and lookback 10..800")
	}
	candles, freshness, err := g.lastBars(ctx, in.Venue, in.Symbol, in.Timeframe, in.Lookback+in.Window+1)
	if err != nil {
		return nil, out, err
	}
	high, low, close, err := ohlc(candles)
	if err != nil {
		return nil, out, err
	}
	v, err := computeVolatility(high, low, close, in.Window, in.Lookback, timeframes[in.Timeframe])
	if err != nil {
		return nil, out, err
	}
	return nil, volatilityOut{Venue: in.Venue, Symbol: in.Symbol, Timeframe: in.Timeframe, Window: in.Window, Lookback: in.Lookback, Bars: len(close), LastClose: close[len(close)-1], Freshness: freshness, volatility: v}, nil
}

// --- get_trend ---

type trendIn struct {
	Venue     string `json:"venue,omitempty" jsonschema:"gateway venue: tbank (default), binance or bybit"`
	Symbol    string `json:"symbol" jsonschema:"instrument symbol"`
	Timeframe string `json:"timeframe" jsonschema:"one of 1m, 5m, 15m, 1h, 1d"`
}

type trend struct {
	EMA20         float64  `json:"ema20"`
	EMA50         float64  `json:"ema50"`
	EMA200        *float64 `json:"ema200,omitempty" jsonschema:"omitted when fewer than 200 bars are available"`
	EMA20SlopePct float64  `json:"ema20_slope_pct" jsonschema:"EMA20 change per bar over the last 5 bars, % of price"`
	ADX           float64  `json:"adx" jsonschema:"ADX(14)"`
	PriceVsEMA200 string   `json:"price_vs_ema200" jsonschema:"above, below or unknown"`
	Label         string   `json:"label" jsonschema:"up, down or flat"`
	Strength      string   `json:"strength" jsonschema:"weak or strong"`
}

type trendOut struct {
	Venue     string  `json:"venue"`
	Symbol    string  `json:"symbol"`
	Timeframe string  `json:"timeframe"`
	Bars      int     `json:"bars" jsonschema:"closed bars used"`
	LastClose float64 `json:"last_close"`
	Freshness string  `json:"freshness"`
	trend
}

func computeTrend(high, low, close []float64) (trend, error) {
	n := len(close)
	if n < minTrendBars {
		return trend{}, &toolError{codeInsufficientData, fmt.Sprintf("%d closed bars, need at least %d", n, minTrendBars)}
	}
	ema20, ema50, ema200 := indicators.EMA(close, 20), indicators.EMA(close, 50), indicators.EMA(close, 200)
	adx := indicators.ADX(high, low, close, 14)[n-1]
	last, prev := ema20[n-1], ema20[n-1-slopeBars]
	if prev == 0 {
		return trend{}, &toolError{codeInsufficientData, "zero price in the EMA window"}
	}
	t := trend{EMA20: last, EMA50: ema50[n-1], EMA20SlopePct: 100 * (last - prev) / prev / slopeBars, ADX: adx, PriceVsEMA200: "unknown", Label: "flat", Strength: "weak"}
	if v := ema200[n-1]; !math.IsNaN(v) {
		t.EMA200 = &v
		t.PriceVsEMA200 = "below"
		if close[n-1] > v {
			t.PriceVsEMA200 = "above"
		}
	}
	c := close[n-1]
	switch {
	case adx < adxTrendMin:
	case c > t.EMA50 && t.EMA20 > t.EMA50 && t.EMA20SlopePct > 0:
		t.Label = "up"
	case c < t.EMA50 && t.EMA20 < t.EMA50 && t.EMA20SlopePct < 0:
		t.Label = "down"
	}
	if adx >= adxStrong {
		t.Strength = "strong"
	}
	return t, nil
}

func (g *gateway) getTrend(ctx context.Context, _ *mcp.CallToolRequest, in trendIn) (*mcp.CallToolResult, trendOut, error) {
	out := trendOut{}
	if err := checkSeries(&in.Venue, in.Symbol, in.Timeframe); err != nil {
		return nil, out, err
	}
	candles, freshness, err := g.lastBars(ctx, in.Venue, in.Symbol, in.Timeframe, trendBars)
	if err != nil {
		return nil, out, err
	}
	high, low, close, err := ohlc(candles)
	if err != nil {
		return nil, out, err
	}
	t, err := computeTrend(high, low, close)
	if err != nil {
		return nil, out, err
	}
	return nil, trendOut{Venue: in.Venue, Symbol: in.Symbol, Timeframe: in.Timeframe, Bars: len(close), LastClose: close[len(close)-1], Freshness: freshness, trend: t}, nil
}

// --- market_snapshot ---

type snapshotIn struct {
	Venue     string   `json:"venue,omitempty" jsonschema:"gateway venue: tbank (default), binance or bybit"`
	Symbols   []string `json:"symbols" jsonschema:"1..20 instrument symbols"`
	Timeframe string   `json:"timeframe" jsonschema:"one of 1m, 5m, 15m, 1h, 1d"`
}

type snapshotItem struct {
	Symbol          string   `json:"symbol"`
	LastClose       *float64 `json:"last_close,omitempty"`
	ChangePct       *float64 `json:"change_pct,omitempty" jsonschema:"last close vs previous bar close, %"`
	VolatilityLabel string   `json:"volatility_label,omitempty"`
	TrendLabel      string   `json:"trend_label,omitempty"`
	Error           string   `json:"error,omitempty" jsonschema:"typed error code for this symbol; other fields are empty"`
	Message         string   `json:"message,omitempty"`
}

type snapshotOut struct {
	Venue     string         `json:"venue"`
	Timeframe string         `json:"timeframe"`
	Items     []snapshotItem `json:"items"`
}

func (g *gateway) marketSnapshot(ctx context.Context, _ *mcp.CallToolRequest, in snapshotIn) (*mcp.CallToolResult, snapshotOut, error) {
	out := snapshotOut{}
	if len(in.Symbols) == 0 || len(in.Symbols) > maxSnapshotItems {
		return nil, out, invalid("symbols must hold 1..%d entries", maxSnapshotItems)
	}
	for _, symbol := range in.Symbols {
		if err := checkSeries(&in.Venue, symbol, in.Timeframe); err != nil {
			return nil, out, err
		}
	}
	step := timeframes[in.Timeframe]
	to := g.now().UnixMilli() / step * step
	from := to - chunkBars(in.Timeframe)*step
	items := make([]batchItem, len(in.Symbols))
	for i, symbol := range in.Symbols {
		items[i] = batchItem{Venue: in.Venue, Symbol: symbol, Timeframe: in.Timeframe, FromUTCMS: from, ToUTCMS: to, Limit: int(chunkBars(in.Timeframe))}
	}
	results, err := g.batch(ctx, items)
	if err != nil {
		return nil, out, err
	}
	out = snapshotOut{Venue: in.Venue, Timeframe: in.Timeframe, Items: make([]snapshotItem, len(results))}
	for i, result := range results {
		item := snapshotItem{Symbol: in.Symbols[i]}
		err := error(nil)
		if result.Error != "" {
			err = &toolError{result.Error, result.Message}
		}
		var high, low, close []float64
		if err == nil {
			high, low, close, err = ohlc(result.Candles)
		}
		var v volatility
		var t trend
		if err == nil {
			v, err = computeVolatility(high, low, close, 14, len(close), step)
		}
		if err == nil {
			t, err = computeTrend(high, low, close)
		}
		if err == nil && close[len(close)-2] == 0 {
			err = &toolError{codeInsufficientData, "zero previous close"}
		}
		var typed *toolError
		if errors.As(err, &typed) {
			item.Error, item.Message = typed.code, typed.message
		} else {
			n := len(close)
			last, change := close[n-1], 100*(close[n-1]-close[n-2])/close[n-2]
			item.LastClose, item.ChangePct = &last, &change
			item.VolatilityLabel, item.TrendLabel = v.Label, t.Label
		}
		out.Items[i] = item
	}
	return nil, out, nil
}

// --- gateway_status ---

type statusIn struct{}

type statusOut struct {
	GatewayURL string   `json:"gateway_url"`
	Health     string   `json:"health" jsonschema:"ok when /v1/health answers"`
	Ready      bool     `json:"ready" jsonschema:"result of /v1/ready"`
	Build      string   `json:"build" jsonschema:"gateway build (node binary version)"`
	NodeID     string   `json:"node_id,omitempty" jsonschema:"cluster node id of the answering gateway; omitted by gateways that do not report it"`
	Venues     []string `json:"venues" jsonschema:"venues the gateway can serve; binance/bybit only when the gateway runs with -crypto-mode"`
	Timeframes []string `json:"timeframes"`
}

func (g *gateway) gatewayStatus(ctx context.Context, _ *mcp.CallToolRequest, _ statusIn) (*mcp.CallToolResult, statusOut, error) {
	var health struct {
		Status, Build string
		NodeID        string `json:"node_id"`
	}
	if err := g.call(ctx, http.MethodGet, "/v1/health", nil, &health); err != nil {
		return nil, statusOut{}, err
	}
	var ready struct{ Status string }
	readyErr := g.call(ctx, http.MethodGet, "/v1/ready", nil, &ready)
	return nil, statusOut{GatewayURL: g.baseURL, Health: health.Status, Ready: readyErr == nil && ready.Status == "ready", Build: health.Build, NodeID: health.NodeID, Venues: []string{"tbank", "binance", "bybit"}, Timeframes: []string{"1m", "5m", "15m", "1h", "1d"}}, nil
}

// --- server ---

func newServer(g *gateway, version string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "market-data-gateway", Version: version}, nil)
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}
	errorsNote := " Errors are tool errors whose text starts with a typed code (INVALID_REQUEST, UNKNOWN_SYMBOL, GATEWAY_UNAVAILABLE, INSUFFICIENT_DATA, or any gateway error code)."
	mcp.AddTool(server, &mcp.Tool{Name: "get_candles", Annotations: readOnly, Description: "Closed OHLCV candles from the market-data gateway. Either lookback (most recent N bars, default 100) or a from/to ISO-8601 window (limit <= 1000)." + errorsNote}, g.getCandles)
	mcp.AddTool(server, &mcp.Tool{Name: "get_volatility", Annotations: readOnly, Description: fmt.Sprintf("Volatility of one instrument: Wilder ATR(window, default 14), ATR %% of last close, realized vol (sample stdev of log returns over lookback, default 200 bars; annualized by sqrt(365 days / timeframe)), and the percentile rank of the current ATR among the lookback ATR values (ties count half). label = low if percentile < %g, high if > %g, else normal.", volLowPercentile, volHighPercentile) + errorsNote}, g.getVolatility)
	mcp.AddTool(server, &mcp.Tool{Name: "get_trend", Annotations: readOnly, Description: fmt.Sprintf("Trend of one instrument over the last %d closed bars: EMA 20/50/200, EMA20 slope (%% per bar over %d bars), Wilder ADX(14), price vs EMA200. label = flat if ADX < %g; up if close > EMA50, EMA20 > EMA50 and slope > 0; down if close < EMA50, EMA20 < EMA50 and slope < 0; otherwise flat. strength = strong if ADX >= %g, else weak. Needs at least %d bars.", trendBars, slopeBars, adxTrendMin, adxStrong, minTrendBars) + errorsNote}, g.getTrend)
	mcp.AddTool(server, &mcp.Tool{Name: "market_snapshot", Annotations: readOnly, Description: fmt.Sprintf("Quick read of up to %d symbols in one gateway batch call (one gateway window per symbol: 168 calendar bars for 1h, 500 otherwise): last close, change %% vs previous bar, volatility label and trend label with the same rules as get_volatility (window 14, lookback = all fetched bars) and get_trend. An unregistered symbol fails the whole call (the gateway validates the batch before any provider call); a per-symbol data failure carries its typed error code instead of values.", maxSnapshotItems) + errorsNote}, g.marketSnapshot)
	mcp.AddTool(server, &mcp.Tool{Name: "gateway_status", Annotations: readOnly, Description: "Gateway health/readiness, build, node id, and the venues and timeframes it serves." + errorsNote}, g.gatewayStatus)
	server.AddPrompt(&mcp.Prompt{
		Name:        "market_brief",
		Description: "Short market read of one instrument from get_trend and get_volatility.",
		Arguments: []*mcp.PromptArgument{
			{Name: "symbol", Description: "instrument symbol, e.g. SBER", Required: true},
			{Name: "timeframe", Description: "1m, 5m, 15m, 1h or 1d", Required: true},
			{Name: "venue", Description: "gateway venue (default tbank)"},
		},
	}, marketBrief)
	return server
}

func marketBrief(_ context.Context, request *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	args := request.Params.Arguments
	venue := args["venue"]
	if venue == "" {
		venue = defaultVenue
	}
	if err := checkSeries(&venue, args["symbol"], args["timeframe"]); err != nil {
		return nil, err
	}
	text := fmt.Sprintf("Call get_trend and get_volatility with venue=%q, symbol=%q, timeframe=%q. Then write a short market brief (3-5 sentences): trend direction and strength, volatility regime, and anything notable (price vs EMA200, data freshness). State only what the tool outputs show; no trading advice.", venue, args["symbol"], args["timeframe"])
	return &mcp.GetPromptResult{Description: "market brief for " + args["symbol"], Messages: []*mcp.PromptMessage{{Role: "user", Content: &mcp.TextContent{Text: text}}}}, nil
}
