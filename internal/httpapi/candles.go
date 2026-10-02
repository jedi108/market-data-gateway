package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/service"
)

type Resolver func(venue, symbol, timeframe string) (model.SeriesKey, error)

// SeriesDescriptor maps a resolved series identity to its physical-instrument
// metadata (F20 futures support). It is keyed by the provider instrument UID —
// the same identity storage keys candles by — never by the logical alias, so
// the metadata cannot disagree with the stored series. The bool reports
// whether the physical instrument is known; unknown keys omit metadata rather
// than invent it.
type SeriesDescriptor func(key model.SeriesKey) (model.InstrumentMetadata, bool)

// IdentityResolver maps a request to its logical client identity and fixed
// scheduler priority (task 109). It returns a typed error for rejected
// credentials; the rejection happens before admission.
type IdentityResolver func(r *http.Request) (clientID string, priority ratelimit.Priority, err error)

// HandlerOption configures optional handler behavior.
type HandlerOption func(*GatewayHandler)

// WithIdentityResolver installs the client identity allowlist. When unset the
// boundary keeps the legacy loopback behavior.
func WithIdentityResolver(fn IdentityResolver) HandlerOption {
	return func(h *GatewayHandler) { h.identity = IdentityResolver(fn) }
}

// WithSeriesDescriptors installs the physical-instrument metadata lookup so
// responses carry explicit identity/units (instrument type, price domain,
// volume unit, futures expiration). When unset, responses keep the legacy
// shape with the field omitted (share back-compat).
func WithSeriesDescriptors(fn SeriesDescriptor) HandlerOption {
	return func(h *GatewayHandler) { h.describe = SeriesDescriptor(fn) }
}

type GatewayHandler struct {
	*HealthHandler
	service       *service.Service
	resolve       Resolver
	maxLimit      int
	maxBatchItems int
	batchFanOut   int
	logger        *slog.Logger
	metrics       *gatewayMetrics
	identity      IdentityResolver
	describe      SeriesDescriptor
}

func NewGatewayHandler(build string, ready func() bool, candles *service.Service, resolve Resolver, maxLimit int) (*GatewayHandler, error) {
	return newGatewayHandler(build, ready, candles, resolve, maxLimit, defaultMaxBatchItems, defaultBatchFanOut)
}

// defaultMaxBatchItems is the hard client batch bound when the composition
// root does not carry a configured value.
const defaultMaxBatchItems = 64

// defaultBatchFanOut mirrors service.defaultBatchFanOut for handler defaults.
const defaultBatchFanOut = 8

// NewGatewayHandlerWithBatchLimits wires the configured batch bounds.
func NewGatewayHandlerWithBatchLimits(build string, ready func() bool, candles *service.Service, resolve Resolver, maxLimit, maxBatchItems, batchFanOut int) (*GatewayHandler, error) {
	return newGatewayHandler(build, ready, candles, resolve, maxLimit, maxBatchItems, batchFanOut)
}

// NewGatewayHandlerWithIdentity wires batch bounds plus handler options such
// as the client identity allowlist (task 109).
func NewGatewayHandlerWithIdentity(build string, ready func() bool, candles *service.Service, resolve Resolver, maxLimit, maxBatchItems, batchFanOut int, opts ...HandlerOption) (*GatewayHandler, error) {
	return newGatewayHandler(build, ready, candles, resolve, maxLimit, maxBatchItems, batchFanOut, opts...)
}

func newGatewayHandler(build string, ready func() bool, candles *service.Service, resolve Resolver, maxLimit, maxBatchItems, batchFanOut int, opts ...HandlerOption) (*GatewayHandler, error) {
	if candles == nil || resolve == nil || maxLimit <= 0 || maxBatchItems <= 0 || batchFanOut <= 0 {
		return nil, fmt.Errorf("candle service, resolver, and positive limits are required")
	}
	h := &GatewayHandler{HealthHandler: NewHealthHandler(build, ready), service: candles, resolve: resolve, maxLimit: maxLimit, maxBatchItems: maxBatchItems, batchFanOut: batchFanOut, logger: slog.Default(), metrics: newGatewayMetrics()}
	for _, opt := range opts {
		opt(h)
	}
	return h, nil
}

// identityFor resolves the caller's client identity and priority. Without an
// installed allowlist it falls back to the legacy advisory X-Client-ID header
// with the shared live-refresh priority (loopback-only boundary).
func (h *GatewayHandler) identityFor(r *http.Request) (string, ratelimit.Priority, error) {
	if h.identity == nil {
		clientID := r.Header.Get("X-Client-ID")
		if clientID == "" {
			clientID = "loopback"
		}
		return clientID, ratelimit.PriorityLiveRefresh, nil
	}
	return h.identity(r)
}

func (h *GatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	switch r.URL.Path {
	case "/v1/candles":
		h.candles(recorder, r)
	case "/v1/candles/batch":
		h.candlesBatch(recorder, r)
	case "/metrics":
		h.serveMetrics(recorder, r)
	default:
		h.HealthHandler.ServeHTTP(recorder, r)
	}
}

// maxBatchBytes bounds the inbound client batch body.
const maxBatchBytes = 8 << 20

// clientBatchItem is one request item of POST /v1/candles/batch.
type clientBatchItem struct {
	Venue             string `json:"venue"`
	Symbol            string `json:"symbol"`
	Timeframe         string `json:"timeframe"`
	FromUTCMS         int64  `json:"from_utc_ms"`
	ToUTCMS           int64  `json:"to_utc_ms"`
	Limit             int    `json:"limit"`
	IncludeIncomplete bool   `json:"include_incomplete"`
}

type clientBatchRequest struct {
	Items []clientBatchItem `json:"items"`
}

func (h *GatewayHandler) candlesBatch(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	venue := "unknown"
	defer func() { h.metrics.recordRequest(venue, strconv.Itoa(statusFromResponse(w)), time.Since(started)) }()
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var batch clientBatchRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxBatchBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&batch); err != nil {
		h.logger.Warn("gateway batch rejected", "reason", "validation")
		h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "batch body is not valid", 0))
		return
	}
	if len(batch.Items) == 0 || len(batch.Items) > h.maxBatchItems {
		h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "batch size is outside configured bounds", 0))
		return
	}
	requests := make([]model.CandleRequest, 0, len(batch.Items))
	for _, item := range batch.Items {
		if item.Venue == "" || item.Symbol == "" || item.Timeframe == "" {
			h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "venue, symbol, and timeframe are required in every batch item", 0))
			return
		}
		if item.ToUTCMS <= item.FromUTCMS || item.FromUTCMS < 0 || item.Limit < 1 || item.Limit > h.maxLimit {
			h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "batch item window or limit is outside bounds", 0))
			return
		}
		if err := validateWindow(item.Timeframe, item.FromUTCMS, item.ToUTCMS, item.Limit); err != nil {
			h.writeError(w, err)
			return
		}
		key, err := h.resolve(item.Venue, item.Symbol, item.Timeframe)
		if err != nil {
			h.writeError(w, err)
			return
		}
		requests = append(requests, model.CandleRequest{Series: key, FromUTCMS: item.FromUTCMS, ToUTCMS: item.ToUTCMS, Limit: item.Limit, IncludeIncomplete: item.IncludeIncomplete})
	}
	venue = requests[0].Series.Venue
	clientID, priority, err := h.identityFor(r)
	if err != nil {
		h.logger.Warn("gateway client identity rejected", "venue", venue, "reason", errorCode(err))
		h.writeError(w, err)
		return
	}
	results := h.service.GetBatch(r.Context(), clientID, priority, requests, service.BatchOptions{MaxFanOut: h.batchFanOut})
	envelope := batchEnvelope{SchemaVersion: 1, Items: make([]batchItemEnvelope, 0, len(results))}
	for _, result := range results {
		item := batchItemEnvelope{Series: result.Series, CacheStatus: result.Result.CacheStatus, Freshness: result.Result.Freshness, SourceFetchedAtUTCMS: result.Result.SourceFetchedAtUTCMS, Replica: result.Result.Replica, ProviderStatus: result.Result.ProviderStatus, StaleCause: errorCode(result.Result.StaleCause), Instrument: h.instrumentMetadata(result.Series)}
		if result.Error != nil {
			item.Error = errorCode(result.Error)
			item.Message = errorMessage(result.Error)
			item.Candles = nil
		} else {
			item.Candles = result.Result.Candles
			h.recordSeriesObservability(result.Series, result.Result)
		}
		envelope.Items = append(envelope.Items, item)
		if result.Error == nil {
			h.metrics.recordResult(result.Series.Venue, result.Series.Timeframe, string(result.Result.CacheStatus), string(result.Result.Freshness))
		}
	}
	h.logger.Debug("gateway batch decision", "items", len(results))
	write(w, http.StatusOK, envelope)
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	var typed *apperror.Error
	if errors.As(err, &typed) {
		return typed.Message
	}
	return "batch item failed"
}

func (h *GatewayHandler) candles(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	venue := "unknown"
	defer func() { h.metrics.recordRequest(venue, strconv.Itoa(statusFromResponse(w)), time.Since(started)) }()
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	request, err := h.parse(r)
	if err != nil {
		h.logger.Warn("gateway request rejected", "reason", "validation")
		h.writeError(w, err)
		return
	}
	venue = request.Series.Venue
	clientID, priority, err := h.identityFor(r)
	if err != nil {
		h.logger.Warn("gateway client identity rejected", "venue", venue, "timeframe", request.Series.Timeframe, "reason", errorCode(err))
		h.writeError(w, err)
		return
	}
	ctx := r.Context()
	result, err := h.service.Get(ctx, clientID, priority, request)
	if err != nil {
		// Admission rejections are sentinel errors, not apperror codes: the
		// typed reason (node_budget/client_quota/queue_full/draining) is the
		// actionable observability contract from task 109.
		reason := errorCode(err)
		if service.IsOverloaded(err) {
			reason = service.AdmissionReason(err)
		}
		if service.IsOverloaded(err) || errors.Is(err, service.ErrIncompleteCoverage) {
			h.logger.Warn("gateway request degraded", "venue", request.Series.Venue, "timeframe", request.Series.Timeframe, "reason", reason)
		} else {
			h.logger.Error("gateway request failed", "venue", request.Series.Venue, "timeframe", request.Series.Timeframe, "reason", reason)
		}
		h.writeError(w, err)
		return
	}
	h.metrics.recordResult(request.Series.Venue, request.Series.Timeframe, string(result.CacheStatus), string(result.Freshness))
	h.recordSeriesObservability(request.Series, result)
	if result.CacheStatus != service.CacheMemoryHit && result.CacheStatus != service.CachePersistentHit {
		h.logger.Debug("gateway candle decision", "venue", request.Series.Venue, "timeframe", request.Series.Timeframe, "cache_status", result.CacheStatus, "freshness", result.Freshness)
	}
	write(w, http.StatusOK, candleEnvelope{SchemaVersion: 1, Series: request.Series, Candles: result.Candles, CacheStatus: result.CacheStatus, Freshness: result.Freshness, SourceFetchedAtUTCMS: result.SourceFetchedAtUTCMS, Replica: result.Replica, ProviderStatus: result.ProviderStatus, StaleCause: errorCode(result.StaleCause), Instrument: h.instrumentMetadata(request.Series)})
}

// instrumentMetadata resolves the physical-instrument metadata of a served
// series, or nil when no descriptor is installed / the physical instrument is
// unknown. Nil keeps the field omitted from the JSON response.
func (h *GatewayHandler) instrumentMetadata(key model.SeriesKey) *model.InstrumentMetadata {
	if h.describe == nil {
		return nil
	}
	if metadata, ok := h.describe(key); ok {
		return &metadata
	}
	return nil
}

// recordSeriesObservability feeds the F20 recorder families from a served
// result: the cache source split by market type and the fetch-provenance age
// of the served snapshot. It is a no-op without a configured recorder.
func (h *GatewayHandler) recordSeriesObservability(key model.SeriesKey, result service.Result) {
	recorder := h.service.Recorder()
	if recorder == nil {
		return
	}
	recorder.RecordSeriesSource(key.Venue, key.MarketType, string(result.CacheStatus))
	if result.SourceFetchedAtUTCMS > 0 {
		if age := time.Now().UnixMilli() - result.SourceFetchedAtUTCMS; age >= 0 {
			recorder.RecordSourceAge(key.Venue, key.Timeframe, float64(age)/1000)
		}
	}
}

func (h *GatewayHandler) parse(r *http.Request) (model.CandleRequest, error) {
	query := r.URL.Query()
	venue, symbol, timeframe := query.Get("venue"), query.Get("symbol"), query.Get("timeframe")
	if venue == "" || symbol == "" || timeframe == "" {
		return model.CandleRequest{}, apperror.New(apperror.CodeInvalidRequest, "venue, symbol, and timeframe are required", 0)
	}
	from, err := parseInt(query.Get("from_utc_ms"))
	if err != nil {
		return model.CandleRequest{}, apperror.New(apperror.CodeInvalidRequest, "from_utc_ms must be an integer", 0)
	}
	to, err := parseInt(query.Get("to_utc_ms"))
	if err != nil || to <= from || from < 0 {
		return model.CandleRequest{}, apperror.New(apperror.CodeInvalidRequest, "from_utc_ms must be less than to_utc_ms", 0)
	}
	limit, err := strconv.Atoi(query.Get("limit"))
	if err != nil || limit < 1 || limit > h.maxLimit {
		return model.CandleRequest{}, apperror.New(apperror.CodeInvalidRequest, "limit is outside configured bounds", 0)
	}
	if err := validateWindow(timeframe, from, to, limit); err != nil {
		return model.CandleRequest{}, err
	}
	includeIncomplete, err := strconv.ParseBool(query.Get("include_incomplete"))
	if err != nil {
		return model.CandleRequest{}, apperror.New(apperror.CodeInvalidRequest, "include_incomplete must be true or false", 0)
	}
	key, err := h.resolve(venue, symbol, timeframe)
	if err != nil {
		return model.CandleRequest{}, err
	}
	return model.CandleRequest{Series: key, FromUTCMS: from, ToUTCMS: to, Limit: limit, IncludeIncomplete: includeIncomplete}, nil
}

func (h *GatewayHandler) serveMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	m := h.service.Metrics()
	snap := budgetSnapshot{}
	if guard := h.service.BudgetGuard(); guard != nil {
		view := guard.Snapshot()
		snap = budgetSnapshot{Present: true, CredentialGroup: view.CredentialGroup, SafeBudget: view.SafeBudget, ConfiguredSum: view.ConfiguredSum, ObservedBudget: view.ObservedBudget, Blocked: !guard.Safe()}
	}
	h.metrics.writePrometheus(w, serviceMetrics{MemoryHits: m.MemoryHits, MemoryMisses: m.MemoryMisses, SingleflightJoins: m.SingleflightJoins, PersistentHits: m.PersistentHits, ReplicaResponses: m.ReplicaResponses, StaleResponses: m.StaleResponses, RefreshQueued: m.RefreshQueued, RefreshDropped: m.RefreshDropped, RefreshDeferred: m.RefreshDeferred, WarmupCompleted: m.WarmupCompleted, WarmupSkipped: m.WarmupSkipped}, h.service.SchedulerSnapshot(), h.service.Recorder(), snap)
}

func (h *GatewayHandler) writeError(w http.ResponseWriter, err error) {
	if service.IsOverloaded(err) {
		// Task 109: Retry-After is computed from the next admissible provider
		// attempt in the budget accounting window — never hardcoded. The
		// draining contract shares the 429 shape with a distinct typed reason
		// and the same computed hint.
		retrySeconds := int64(1)
		if next := h.service.SchedulerNextAllowedAt(); !next.IsZero() {
			if waitMS := time.Until(next).Milliseconds(); waitMS > 0 {
				retrySeconds = max(1, (waitMS+999)/1000)
			}
		}
		w.Header().Set("Retry-After", strconv.FormatInt(retrySeconds, 10))
		write(w, http.StatusTooManyRequests, errorEnvelope{Error: "GATEWAY_OVERLOADED", Message: "gateway admission limit reached", Reason: service.AdmissionReason(err), Retryable: true})
		return
	}
	var typed *apperror.Error
	if errors.As(err, &typed) {
		if typed.RetryAfterMS > 0 {
			w.Header().Set("Retry-After", strconv.FormatInt(max(1, typed.RetryAfterMS/int64(time.Second/time.Millisecond)), 10))
		}
		write(w, typed.HTTPStatus(), errorEnvelope{Error: string(typed.Code), Message: typed.Message, Retryable: typed.Retryable})
		return
	}
	write(w, http.StatusServiceUnavailable, errorEnvelope{Error: "PROVIDER_UNAVAILABLE", Message: "candle request failed", Retryable: true})
}

type candleEnvelope struct {
	SchemaVersion        int                 `json:"schema_version"`
	Series               model.SeriesKey     `json:"series"`
	Candles              []model.Candle      `json:"candles"`
	CacheStatus          service.CacheStatus `json:"cache_status"`
	Freshness            service.Freshness   `json:"freshness"`
	SourceFetchedAtUTCMS int64               `json:"source_fetched_at_utc_ms"`
	Replica              bool                `json:"replica"`
	ProviderStatus       string              `json:"provider_status"`
	StaleCause           string              `json:"stale_cause,omitempty"`
	// Instrument is the physical-instrument identity/units metadata (F20). It
	// is omitted when no descriptor is installed, keeping the legacy share
	// response shape byte-compatible.
	Instrument *model.InstrumentMetadata `json:"instrument,omitempty"`
}

// batchEnvelope is the always-200 client batch envelope: per-item
// status/error/freshness is independent; transport/auth validation of the
// envelope itself can still reject the whole request before this shape.
type batchEnvelope struct {
	SchemaVersion int                 `json:"schema_version"`
	Items         []batchItemEnvelope `json:"items"`
}

type batchItemEnvelope struct {
	Series               model.SeriesKey     `json:"series"`
	Candles              []model.Candle      `json:"candles"`
	CacheStatus          service.CacheStatus `json:"cache_status"`
	Freshness            service.Freshness   `json:"freshness"`
	SourceFetchedAtUTCMS int64               `json:"source_fetched_at_utc_ms"`
	Replica              bool                `json:"replica"`
	ProviderStatus       string              `json:"provider_status"`
	StaleCause           string              `json:"stale_cause,omitempty"`
	Error                string              `json:"error,omitempty"`
	Message              string              `json:"message,omitempty"`
	// Instrument carries the same physical-identity/units metadata as the
	// single-request envelope, per item (F20).
	Instrument *model.InstrumentMetadata `json:"instrument,omitempty"`
}
type errorEnvelope struct {
	Error     string `json:"error"`
	Message   string `json:"message"`
	Reason    string `json:"reason,omitempty"`
	Retryable bool   `json:"retryable"`
}

func parseInt(value string) (int64, error) { return strconv.ParseInt(value, 10, 64) }

// validateWindow keeps the TBank 1h contract bounded and aligned with the
// provider's candle grid. Unaligned windows otherwise produce a valid-looking
// response that can never cover the requested half-open interval, causing
// repeated incomplete_coverage retries. Other timeframes retain the legacy
// generic contract used by existing clients.
func validateWindow(timeframe string, from, to int64, limit int) error {
	if timeframe != "1h" {
		return nil
	}
	const hourMS int64 = 60 * 60 * 1000
	const maxProviderSpanMS int64 = 7 * 24 * hourMS
	if from%hourMS != 0 || to%hourMS != 0 {
		return apperror.New(apperror.CodeInvalidRequest, "1h window must align to UTC hour boundaries", 0)
	}
	span := to - from
	if span <= 0 || span > maxProviderSpanMS || span > int64(limit)*hourMS {
		return apperror.New(apperror.CodeInvalidRequest, "1h window exceeds bounded candle coverage", 0)
	}
	return nil
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	if code := service.TypedCauseCode(err); code != "" {
		return code
	}
	var typed *apperror.Error
	if errors.As(err, &typed) {
		return string(typed.Code)
	}
	return "UPSTREAM_ERROR"
}
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(body []byte) (int, error) {
	return w.ResponseWriter.Write(body)
}

func statusFromResponse(w http.ResponseWriter) int {
	if recorder, ok := w.(*statusRecorder); ok {
		return recorder.status
	}
	return http.StatusOK
}
