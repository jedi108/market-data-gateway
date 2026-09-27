// Package peerapi serves the private peer boundary. The listener is expected
// to be bound to the confirmed private address only (validated in config);
// this handler additionally enforces node authentication, cluster identity,
// routing version, deterministic ownership, and the one-hop constraint on
// candle requests. It never proxies: a request this node does not own is
// rejected with NOT_OWNER, which is what prevents loops. It also serves the
// Phase 6 lease control plane (witness role) under the same node auth:
// /internal/v1/lease/peek and /internal/v1/lease/acquire, which are read-only
// and fencing-guarded respectively and never trigger provider work.
package peerapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/auth"
	"github.com/jedi108/market-data-gateway/internal/cluster"
	"github.com/jedi108/market-data-gateway/internal/lease"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// CandleSource is the local serving capability behind the peer boundary: on
// the owner it is the candle service; tests substitute fakes.
type CandleSource interface {
	// GetOwned serves a series the local node owns; implementations must not
	// recurse into peer calls.
	GetOwned(request model.CandleRequest) (cluster.OwnedResult, error)
	// GetOwnedBatch serves a group of owned series positionally; per-item
	// failures are reported per item and must not fail the whole sub-batch.
	// Implementations must not recurse into peer calls.
	GetOwnedBatch(requests []model.CandleRequest) []cluster.OwnedBatchItem
}

// Handler serves /internal/v1/*.
type Handler struct {
	auth     *auth.NodeAuth
	member   *cluster.Membership
	source   CandleSource
	nodeInfo cluster.NodeInfo
	logger   *slog.Logger
	// witness is the lease quorum store. Nil when failover_mode is manual; when
	// set, the handler serves /internal/v1/lease/* for candidates. The witness
	// store never performs provider work and never affects candle serving.
	witness *lease.WitnessStore
}

// New constructs the peer handler. ready is consulted for /internal/v1/node
// only; it never gates candle serving (a draining-but-ready owner must still
// finish in-flight peer work). witness may be nil (manual failover).
func New(nodeAuth *auth.NodeAuth, member *cluster.Membership, source CandleSource, info cluster.NodeInfo, logger *slog.Logger, witness *lease.WitnessStore) (*Handler, error) {
	if nodeAuth == nil || member == nil || source == nil {
		return nil, fmt.Errorf("node auth, membership, and candle source are required")
	}
	if !nodeAuth.Enabled() {
		return nil, fmt.Errorf("peer boundary requires a configured node credential (fail-closed)")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{auth: nodeAuth, member: member, source: source, nodeInfo: info, logger: logger, witness: witness}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Authentication precedes everything: no cluster/version/owner
	// information is revealed to unauthenticated callers.
	if err := h.auth.Authenticate(r); err != nil {
		h.writeError(w, err)
		return
	}
	switch r.URL.Path {
	case "/internal/v1/candles":
		h.candles(w, r)
	case "/internal/v1/candles/batch":
		h.candlesBatch(w, r)
	case "/internal/v1/node":
		h.node(w, r)
	case "/internal/v1/lease/peek":
		h.leasePeek(w, r)
	case "/internal/v1/lease/acquire":
		h.leaseAcquire(w, r)
	default:
		http.NotFound(w, r)
	}
}

// maxPeerRequestBody bounds the inbound peer payload.
const maxPeerRequestBody = 4 << 20

func (h *Handler) candles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request cluster.PeerCandlesRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxPeerRequestBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "peer request body is not valid", 0))
		return
	}
	// Cluster identity: a foreign cluster is rejected before version checks.
	if request.ClusterID != h.member.ClusterID {
		h.writeError(w, apperror.New(apperror.CodeClusterMismatch, "peer request belongs to a different cluster", 0))
		return
	}
	// Source must be a known member.
	if _, known := h.member.Table().Node(request.SourceNodeID); !known {
		h.writeError(w, apperror.New(apperror.CodeUnknownNode, "peer source is not a cluster member", 0))
		return
	}
	// One-hop only: exactly one hop from the authenticated source. Anything
	// else is a forwarding attempt (loop) and is refused without a provider
	// or peer call.
	if request.HopCount != 1 {
		h.writeError(w, apperror.New(apperror.CodeInvalidHop, "peer requests must arrive with hop_count=1", 0))
		return
	}
	// Routing version must match exactly; a mismatch means concurrent
	// activation states and must not trigger any upstream work.
	if request.RoutingVersion != h.member.RoutingVersion {
		h.writeError(w, apperror.New(apperror.CodeRoutingVersionMismatch, "peer routing version does not match local routing version", 0))
		return
	}
	// Owner verification: the deterministic owner of this series must be
	// this node. NOT_OWNER is terminal — this handler never proxies.
	assignment, err := h.member.Owner(request.Series)
	if err != nil {
		h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "peer series key is invalid", 0))
		return
	}
	if assignment.Primary != h.member.NodeID {
		h.logger.Warn("peer request rejected: not the deterministic owner",
			"venue", request.Series.Venue, "timeframe", request.Series.Timeframe,
			"owner", assignment.Primary, "routing_version", h.member.RoutingVersion)
		h.writeError(w, apperror.New(apperror.CodeNotOwner, "this node is not the owner of the requested series", 0))
		return
	}
	if request.FromUTCMS < 0 || request.ToUTCMS <= request.FromUTCMS || request.Limit <= 0 {
		h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "peer request window or limit is invalid", 0))
		return
	}
	result, err := h.source.GetOwned(model.CandleRequest{
		Series:            request.Series,
		FromUTCMS:         request.FromUTCMS,
		ToUTCMS:           request.ToUTCMS,
		Limit:             request.Limit,
		IncludeIncomplete: request.IncludeIncomplete,
	})
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cluster.PeerCandlesResponse{
		SchemaVersion:        1,
		NodeID:               h.member.NodeID,
		Candles:              result.Candles,
		SourceFetchedAtUTCMS: result.SourceFetchedAtUTCMS,
	})
}

// maxPeerBatchItems bounds one peer sub-batch; the composition root carries
// the configured client batch bound, and the peer bound must never be
// smaller than a legitimate client batch split across two owners.
const maxPeerBatchItems = 128

// candlesBatch serves POST /internal/v1/candles/batch. Transport/auth checks
// mirror the single-request path exactly; ownership is re-verified per item
// so a mis-partitioned or racing batch gets NOT_OWNER per item without any
// provider or peer work for those items.
func (h *Handler) candlesBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request cluster.PeerBatchRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxPeerRequestBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "peer batch request body is not valid", 0))
		return
	}
	if request.ClusterID != h.member.ClusterID {
		h.writeError(w, apperror.New(apperror.CodeClusterMismatch, "peer batch request belongs to a different cluster", 0))
		return
	}
	if _, known := h.member.Table().Node(request.SourceNodeID); !known {
		h.writeError(w, apperror.New(apperror.CodeUnknownNode, "peer batch source is not a cluster member", 0))
		return
	}
	if request.HopCount != 1 {
		h.writeError(w, apperror.New(apperror.CodeInvalidHop, "peer batch requests must arrive with hop_count=1", 0))
		return
	}
	if request.RoutingVersion != h.member.RoutingVersion {
		h.writeError(w, apperror.New(apperror.CodeRoutingVersionMismatch, "peer batch routing version does not match local routing version", 0))
		return
	}
	if len(request.Requests) == 0 || len(request.Requests) > maxPeerBatchItems {
		h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "peer batch size is outside bounds", 0))
		return
	}

	// Per-item validation and ownership re-verification. Owned items are
	// collected with their positional index; non-owned items get a typed
	// per-item NOT_OWNER and are never proxied (loop prevention, batch form).
	owned := make([]model.CandleRequest, 0, len(request.Requests))
	ownedIndex := make([]int, 0, len(request.Requests))
	items := make([]cluster.PeerBatchItem, len(request.Requests))
	for i, item := range request.Requests {
		if err := item.Series.Validate(); err != nil {
			items[i].Error = &cluster.PeerBatchItemError{Code: string(apperror.CodeInvalidRequest), Message: "peer batch item series key is invalid"}
			continue
		}
		if item.FromUTCMS < 0 || item.ToUTCMS <= item.FromUTCMS || item.Limit <= 0 {
			items[i].Error = &cluster.PeerBatchItemError{Code: string(apperror.CodeInvalidRequest), Message: "peer batch item window or limit is invalid"}
			continue
		}
		assignment, err := h.member.Owner(item.Series)
		if err != nil {
			items[i].Error = &cluster.PeerBatchItemError{Code: string(apperror.CodeInvalidRequest), Message: "peer batch item series key is invalid"}
			continue
		}
		if assignment.Primary != h.member.NodeID {
			h.logger.Warn("peer batch item rejected: not the deterministic owner",
				"venue", item.Series.Venue, "timeframe", item.Series.Timeframe,
				"owner", assignment.Primary, "routing_version", h.member.RoutingVersion)
			items[i].Error = &cluster.PeerBatchItemError{Code: string(apperror.CodeNotOwner), Message: "this node is not the owner of the requested series"}
			continue
		}
		owned = append(owned, item)
		ownedIndex = append(ownedIndex, i)
	}

	if len(owned) > 0 {
		for offset, result := range h.source.GetOwnedBatch(owned) {
			position := ownedIndex[offset]
			if result.Error != nil {
				items[position].Error = peerBatchItemError(result.Error)
				continue
			}
			items[position].Candles = result.Result.Candles
			items[position].SourceFetchedAtUTCMS = result.Result.SourceFetchedAtUTCMS
		}
	}
	writeJSON(w, http.StatusOK, cluster.PeerBatchResponse{SchemaVersion: 1, NodeID: h.member.NodeID, Items: items})
}

// peerBatchItemError converts a typed error into its wire form.
func peerBatchItemError(err error) *cluster.PeerBatchItemError {
	var typed *apperror.Error
	if errors.As(err, &typed) {
		return &cluster.PeerBatchItemError{Code: string(typed.Code), Message: typed.Message, Retryable: typed.Retryable}
	}
	return &cluster.PeerBatchItemError{Code: "PEER_INTERNAL", Message: "peer batch item failed", Retryable: false}
}

func (h *Handler) node(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, h.nodeInfo)
}

// leasePeek is the read-only quorum member endpoint: it returns the committed
// lease state for a series without mutating anything. It is part of the Phase 6
// control plane and is served only when this node is configured as a witness.
func (h *Handler) leasePeek(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.witness == nil {
		// A non-witness node refuses lease control-plane traffic entirely. A
		// candidate must not treat a missing witness as a vote.
		h.writeError(w, apperror.New(apperror.CodeLeaseUnavailable, "node is not a lease witness", 0))
		return
	}
	seriesKey := r.URL.Query().Get("series")
	if seriesKey == "" {
		h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "series query parameter is required", 0))
		return
	}
	st := h.witness.Peek(seriesKey)
	writeJSON(w, http.StatusOK, leaseStateEnvelope{
		Series:    seriesKey,
		Owner:     st.Owner,
		Token:     uint64(st.Token),
		Version:   st.Version,
		ExpiresAt: st.ExpiresAt,
		IssuedAt:  st.IssuedAt,
	})
}

// leaseAcquire is the fencing-guarded quorum member endpoint. It delegates to
// the witness store, which enforces forward-only tokens and refuses a live
// conflicting owner. It never triggers provider work.
// leaseAcquire is the fencing-guarded quorum member endpoint. It delegates to
// the witness store, which enforces forward-only tokens and refuses a live
// conflicting owner. It never triggers provider work.
func (h *Handler) leaseAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.witness == nil {
		h.writeError(w, apperror.New(apperror.CodeLeaseUnavailable, "node is not a lease witness", 0))
		return
	}
	var body leaseAcquireRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxPeerRequestBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		h.writeError(w, apperror.New(apperror.CodeInvalidRequest, "lease acquire body is not valid", 0))
		return
	}
	st, err := h.witness.Acquire(body.Series, body.Holder, body.Version, body.TTLMS, lease.Token(body.Token))
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, leaseStateEnvelope{
		Series:    body.Series,
		Owner:     st.Owner,
		Token:     uint64(st.Token),
		Version:   st.Version,
		ExpiresAt: st.ExpiresAt,
		IssuedAt:  st.IssuedAt,
	})
}

// leaseAcquireRequest is the wire contract for POST /internal/v1/lease/acquire.
type leaseAcquireRequest struct {
	Series  string `json:"series"`
	Holder  string `json:"holder"`
	Version int    `json:"version"`
	TTLMS   int64  `json:"ttl_ms"`
	Token   uint64 `json:"token"`
}

// leaseStateEnvelope is the wire contract for lease peek/acquire responses.
type leaseStateEnvelope struct {
	Series    string `json:"series"`
	Owner     string `json:"owner"`
	Token     uint64 `json:"token"`
	Version   int    `json:"version"`
	ExpiresAt int64  `json:"expires_at"`
	IssuedAt  int64  `json:"issued_at"`
}

func (h *Handler) writeError(w http.ResponseWriter, err error) {
	var typed *apperror.Error
	if errors.As(err, &typed) {
		writeJSON(w, typed.HTTPStatus(), peerErrorEnvelope{Error: string(typed.Code), Message: typed.Message, Retryable: typed.Retryable})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, peerErrorEnvelope{Error: "PEER_INTERNAL", Message: "peer request failed", Retryable: false})
}

type peerErrorEnvelope struct {
	Error     string `json:"error"`
	Message   string `json:"Message"`
	Retryable bool   `json:"Retryable"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
