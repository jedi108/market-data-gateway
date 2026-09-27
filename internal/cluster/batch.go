package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// PeerBatchRequest is the wire contract for POST /internal/v1/candles/batch.
// Every item in Requests must resolve to the same deterministic owner — the
// sender partitions by owner before sending, and the owner re-verifies each
// item and rejects non-owned items individually (NOT_OWNER per item, never a
// proxy), which keeps the one-hop/no-loop contract intact for batches.
type PeerBatchRequest struct {
	ClusterID       string                `json:"cluster_id"`
	SourceNodeID    string                `json:"source_node_id"`
	RoutingVersion  int                   `json:"routing_version"`
	HopCount        int                   `json:"hop_count"`
	OriginatedAtUTC int64                 `json:"originated_at_utc_ms"`
	Requests        []model.CandleRequest `json:"requests"`
}

// PeerBatchItem is one per-item outcome in a peer batch response.
type PeerBatchItem struct {
	Candles              []model.Candle `json:"candles"`
	SourceFetchedAtUTCMS int64          `json:"source_fetched_at_utc_ms"`
	// Error is the typed per-item failure (marshalled apperror code/message)
	// or null on success. It mirrors the single-request error taxonomy.
	Error *PeerBatchItemError `json:"error,omitempty"`
}

// PeerBatchItemError is the JSON form of a per-item typed failure.
type PeerBatchItemError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// PeerBatchResponse is the owner-side reply for one sub-batch.
type PeerBatchResponse struct {
	SchemaVersion int             `json:"schema_version"`
	NodeID        string          `json:"node_id"`
	Items         []PeerBatchItem `json:"items"`
}

// OwnedBatchResult is the service-facing result of one peer sub-batch: items
// align positionally with the requests slice of the call.
type OwnedBatchResult struct {
	Items []OwnedBatchItem
}

// OwnedBatchItem is one item of OwnedBatchResult.
type OwnedBatchItem struct {
	Result OwnedResult
	Error  error
}

// maxPeerBatchBodyBytes bounds the peer batch response body.
const maxPeerBatchBodyBytes = 256 << 20

// FetchCandlesBatch makes exactly one peer hop carrying every item for one
// owner. It never retries past the deadline and never contacts a second node.
func (c *Client) FetchCandlesBatch(ctx context.Context, requests []model.CandleRequest) (PeerBatchResponse, error) {
	if len(requests) == 0 {
		return PeerBatchResponse{}, nil
	}
	// The transport deadline is the peer timeout; the caller's context bounds
	// the whole batch.
	callCtx, cancel := context.WithTimeout(ctx, c.http.Timeout)
	defer cancel()
	// All items must share one owner; the client routes by the first item's
	// owner (the sender has already grouped them and the owner re-verifies
	// every item anyway).
	assignment, err := c.membership.Owner(requests[0].Series)
	if err != nil {
		return PeerBatchResponse{}, err
	}
	for _, request := range requests[1:] {
		other, err := c.membership.Owner(request.Series)
		if err != nil {
			return PeerBatchResponse{}, err
		}
		if other.Primary != assignment.Primary {
			return PeerBatchResponse{}, apperror.New(apperror.CodeInvalidRequest, "peer batch items must share one owner", 0)
		}
	}
	if assignment.Primary == c.membership.NodeID {
		return PeerBatchResponse{}, apperror.New(apperror.CodeNotOwner, "local node is the owner; peer call is not required", 0)
	}
	peerURL, ok := c.membership.PeerURL(assignment.Primary)
	if !ok {
		return PeerBatchResponse{}, apperror.New(apperror.CodeUnknownNode, "owner has no peer url", 0)
	}
	payload := PeerBatchRequest{
		ClusterID:       c.membership.ClusterID,
		SourceNodeID:    c.membership.NodeID,
		RoutingVersion:  c.membership.RoutingVersion,
		HopCount:        1,
		OriginatedAtUTC: time.Now().UTC().UnixMilli(),
		Requests:        requests,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return PeerBatchResponse{}, err
	}
	httpRequest, err := http.NewRequestWithContext(callCtx, http.MethodPost, peerURL+"/internal/v1/candles/batch", bytes.NewReader(body))
	if err != nil {
		return PeerBatchResponse{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.http.Do(httpRequest)
	if err != nil {
		return PeerBatchResponse{}, apperror.New(apperror.CodePeerUnavailable, "peer batch call failed", 0)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return PeerBatchResponse{}, decodePeerError(response.Body, assignment.Primary)
	}
	var decoded PeerBatchResponse
	decoded.Items = nil
	if err := json.NewDecoder(io.LimitReader(response.Body, maxPeerBatchBodyBytes)).Decode(&decoded); err != nil {
		return PeerBatchResponse{}, apperror.New(apperror.CodePeerUnavailable, "peer batch response is not readable", 0)
	}
	if decoded.NodeID != assignment.Primary {
		return PeerBatchResponse{}, apperror.New(apperror.CodeNotOwner, "peer batch response came from a non-owner node", 0)
	}
	if len(decoded.Items) != len(requests) {
		return PeerBatchResponse{}, apperror.New(apperror.CodePeerUnavailable, "peer batch response item count mismatch", 0)
	}
	return decoded, nil
}

// PeerBatchFetch adapts FetchCandlesBatch to the service.PeerClient contract.
func (c *Client) PeerBatchFetch(ctx context.Context, requests []model.CandleRequest) (OwnedBatchResult, error) {
	response, err := c.FetchCandlesBatch(ctx, requests)
	if err != nil {
		return OwnedBatchResult{}, err
	}
	items := make([]OwnedBatchItem, 0, len(response.Items))
	for _, item := range response.Items {
		converted := OwnedBatchItem{Result: OwnedResult{Candles: item.Candles, SourceFetchedAtUTCMS: item.SourceFetchedAtUTCMS}}
		if item.Error != nil {
			converted.Error = apperror.New(apperror.Code(item.Error.Code), item.Error.Message, 0)
		}
		items = append(items, converted)
	}
	return OwnedBatchResult{Items: items}, nil
}
