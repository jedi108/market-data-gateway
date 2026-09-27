// Package cluster binds membership, deterministic routing, and the one-hop
// peer client into the node's view of the cluster. It owns nothing else:
// no health state, no automatic failover, no provider access.
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/lease"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/routing"
)

// Membership is the local node's frozen view of the cluster.
type Membership struct {
	ClusterID      string
	NodeID         string
	RoutingVersion int
	RoutingHash    string
	table          *routing.Table
	peerURLs       map[string]string
}

// NewMembership builds the view from validated configuration.
func NewMembership(clusterID, nodeID string, routingVersion int, nodes []routing.Node, peerURLs map[string]string, routingHash string) (*Membership, error) {
	table, err := routing.NewTable(routingVersion, nodes)
	if err != nil {
		return nil, err
	}
	if clusterID == "" || nodeID == "" {
		return nil, fmt.Errorf("cluster id and node id are required")
	}
	if _, ok := table.Node(nodeID); !ok {
		return nil, fmt.Errorf("local node %q is not in membership", nodeID)
	}
	for _, id := range table.NodeIDs() {
		if peerURLs[id] == "" {
			return nil, fmt.Errorf("peer url for node %q is required", id)
		}
	}
	urls := make(map[string]string, len(peerURLs))
	for id, url := range peerURLs {
		urls[id] = strings.TrimSuffix(url, "/")
	}
	return &Membership{ClusterID: clusterID, NodeID: nodeID, RoutingVersion: routingVersion, RoutingHash: routingHash, table: table, peerURLs: urls}, nil
}

// Table exposes the immutable routing table.
func (m *Membership) Table() *routing.Table { return m.table }

// PeerURL returns the configured private peer endpoint of a member.
func (m *Membership) PeerURL(nodeID string) (string, bool) {
	url, ok := m.peerURLs[nodeID]
	return url, ok
}

// Owner returns the deterministic primary owner of a series.
func (m *Membership) Owner(key model.SeriesKey) (routing.Assignment, error) {
	return m.table.Assign(key)
}

// IsLocalOwner reports whether this node owns the series upstream.
func (m *Membership) IsLocalOwner(key model.SeriesKey) (bool, error) {
	assignment, err := m.Owner(key)
	if err != nil {
		return false, err
	}
	return assignment.Primary == m.NodeID, nil
}

// OwnerOf returns the primary owner node id of a series, matching the
// service.Router contract: the local node's id when this node owns the
// series, otherwise the remote owner's id.
func (m *Membership) OwnerOf(key model.SeriesKey) (string, error) {
	assignment, err := m.Owner(key)
	if err != nil {
		return "", err
	}
	return assignment.Primary, nil
}

// OwnedResult is the owner-side result of serving one peer candle request.
type OwnedResult struct {
	Candles              []model.Candle
	SourceFetchedAtUTCMS int64
}

// NodeInfo is the non-secret identity payload served by /internal/v1/node.
type NodeInfo struct {
	NodeID         string `json:"node_id"`
	ClusterID      string `json:"cluster_id"`
	RoutingVersion int    `json:"routing_version"`
	RoutingHash    string `json:"routing_hash"`
	Build          string `json:"build"`
	Ready          bool   `json:"ready"`
}

// PeerCandlesRequest is the wire contract for POST /internal/v1/candles.
// HopCount is always 1 from a source node; the owner must never forward.
type PeerCandlesRequest struct {
	ClusterID         string          `json:"cluster_id"`
	SourceNodeID      string          `json:"source_node_id"`
	RoutingVersion    int             `json:"routing_version"`
	HopCount          int             `json:"hop_count"`
	OriginatedAtUTC   int64           `json:"originated_at_utc_ms"`
	Series            model.SeriesKey `json:"series"`
	FromUTCMS         int64           `json:"from_utc_ms"`
	ToUTCMS           int64           `json:"to_utc_ms"`
	Limit             int             `json:"limit"`
	IncludeIncomplete bool            `json:"include_incomplete"`
}

// PeerCandlesResponse mirrors the client envelope, plus the owner's view of
// provenance so the source node can cache the replica safely.
type PeerCandlesResponse struct {
	SchemaVersion        int            `json:"schema_version"`
	NodeID               string         `json:"node_id"`
	Candles              []model.Candle `json:"candles"`
	SourceFetchedAtUTCMS int64          `json:"source_fetched_at_utc_ms"`
}

// Client calls the peer API of remote owners. One instance is shared; the
// underlying transport must have bounded timeouts.
type Client struct {
	membership *Membership
	token      string
	http       *http.Client
}

// NewClient builds the peer client. The node token is held in memory only.
func NewClient(m *Membership, nodeToken string, timeout time.Duration) (*Client, error) {
	if m == nil {
		return nil, fmt.Errorf("membership is required")
	}
	if nodeToken == "" {
		return nil, fmt.Errorf("node token is required for peer calls")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("peer timeout must be positive")
	}
	return &Client{membership: m, token: nodeToken, http: &http.Client{Timeout: timeout}}, nil
}

// FetchCandles makes exactly one peer hop to the deterministic owner of the
// series. It never retries past the deadline and never contacts a second node.
func (c *Client) FetchCandles(ctx context.Context, request model.CandleRequest) (PeerCandlesResponse, error) {
	assignment, err := c.membership.Owner(request.Series)
	if err != nil {
		return PeerCandlesResponse{}, err
	}
	if assignment.Primary == c.membership.NodeID {
		return PeerCandlesResponse{}, apperror.New(apperror.CodeNotOwner, "local node is the owner; peer call is not required", 0)
	}
	peerURL, ok := c.membership.PeerURL(assignment.Primary)
	if !ok {
		return PeerCandlesResponse{}, apperror.New(apperror.CodeUnknownNode, "owner has no peer url", 0)
	}
	payload := PeerCandlesRequest{
		ClusterID:         c.membership.ClusterID,
		SourceNodeID:      c.membership.NodeID,
		RoutingVersion:    c.membership.RoutingVersion,
		HopCount:          1,
		OriginatedAtUTC:   time.Now().UTC().UnixMilli(),
		Series:            request.Series,
		FromUTCMS:         request.FromUTCMS,
		ToUTCMS:           request.ToUTCMS,
		Limit:             request.Limit,
		IncludeIncomplete: request.IncludeIncomplete,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return PeerCandlesResponse{}, err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.http.Timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(callCtx, http.MethodPost, peerURL+"/internal/v1/candles", bytes.NewReader(body))
	if err != nil {
		return PeerCandlesResponse{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.http.Do(httpRequest)
	if err != nil {
		return PeerCandlesResponse{}, apperror.New(apperror.CodePeerUnavailable, "peer call failed", 0)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return PeerCandlesResponse{}, decodePeerError(response.Body, assignment.Primary)
	}
	var decoded PeerCandlesResponse
	decoded.Candles = nil
	if err := json.NewDecoder(io.LimitReader(response.Body, maxPeerBodyBytes)).Decode(&decoded); err != nil {
		return PeerCandlesResponse{}, apperror.New(apperror.CodePeerUnavailable, "peer response is not readable", 0)
	}
	if decoded.NodeID != assignment.Primary {
		return PeerCandlesResponse{}, apperror.New(apperror.CodeNotOwner, "peer response came from a non-owner node", 0)
	}
	return decoded, nil
}

// maxPeerBodyBytes bounds the peer response body to keep a misbehaving peer
// from exhausting memory. The limit comfortably exceeds max_request_limit
// candles for any supported timeframe.
const maxPeerBodyBytes = 64 << 20

type peerErrorBody struct {
	Error     string `json:"error"`
	Message   string `json:"Message"`
	Retryable bool   `json:"Retryable"`
}

func decodePeerError(body io.Reader, owner string) error {
	var decoded peerErrorBody
	raw, err := io.ReadAll(io.LimitReader(body, 1<<20))
	if err == nil {
		_ = json.Unmarshal(raw, &decoded)
	}
	if decoded.Error == "" {
		return apperror.New(apperror.CodePeerUnavailable, "peer returned an unexpected status", 0)
	}
	code := apperror.Code(decoded.Error)
	switch code {
	case apperror.CodeRoutingVersionMismatch, apperror.CodeNotOwner, apperror.CodeNodeUnauthenticated, apperror.CodeClusterMismatch, apperror.CodeUnknownNode, apperror.CodeInvalidHop, apperror.CodePeerUnavailable:
		return apperror.New(code, decoded.Message, 0)
	default:
		// Provider-side failures propagate with their own taxonomy; the peer
		// is the transport, not the failure domain.
		return apperror.New(code, decoded.Message, 0)
	}
}

// Node fetches /internal/v1/node from a member for switch/verify procedures.
func (c *Client) Node(ctx context.Context, nodeID string) (NodeInfo, error) {
	peerURL, ok := c.membership.PeerURL(nodeID)
	if !ok {
		return NodeInfo{}, apperror.New(apperror.CodeUnknownNode, "node has no peer url", 0)
	}
	callCtx, cancel := context.WithTimeout(ctx, c.http.Timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(callCtx, http.MethodGet, peerURL+"/internal/v1/node", nil)
	if err != nil {
		return NodeInfo{}, err
	}
	httpRequest.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.http.Do(httpRequest)
	if err != nil {
		return NodeInfo{}, apperror.New(apperror.CodePeerUnavailable, "peer node query failed", 0)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return NodeInfo{}, decodePeerError(response.Body, nodeID)
	}
	var info NodeInfo
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&info); err != nil {
		return NodeInfo{}, apperror.New(apperror.CodePeerUnavailable, "peer node info is not readable", 0)
	}
	return info, nil
}

// PeerFetch adapts FetchCandles to the service.PeerClient contract.
func (c *Client) PeerFetch(ctx context.Context, request model.CandleRequest) (OwnedResult, error) {
	response, err := c.FetchCandles(ctx, request)
	if err != nil {
		return OwnedResult{}, err
	}
	return OwnedResult{Candles: response.Candles, SourceFetchedAtUTCMS: response.SourceFetchedAtUTCMS}, nil
}

// IsPeerError reports whether err originates from the peer boundary rather
// than the provider boundary.
func IsPeerError(err error) bool {
	var typed *apperror.Error
	if !errors.As(err, &typed) {
		return false
	}
	switch typed.Code {
	case apperror.CodeNotOwner, apperror.CodeRoutingVersionMismatch, apperror.CodeNodeUnauthenticated, apperror.CodeClusterMismatch, apperror.CodeUnknownNode, apperror.CodeInvalidHop, apperror.CodePeerUnavailable:
		return true
	}
	return false
}

// RemoteWitness is a lease.Member backed by a remote witness node's peer
// boundary. It implements the Phase 6 quorum member contract over HTTP: Peek
// is a read-only GET, Acquire is a fencing-guarded POST. A transport failure is
// reported as LEASE_UNAVAILABLE so the coordinator never counts an unreachable
// witness as a vote — a minority partition therefore cannot reach quorum.
type RemoteWitness struct {
	client *Client
	nodeID string
}

// NewRemoteWitness builds a quorum member for the witness nodeID.
func NewRemoteWitness(c *Client, nodeID string) *RemoteWitness {
	return &RemoteWitness{client: c, nodeID: nodeID}
}

// Peek returns the committed lease state from the remote witness.
func (w *RemoteWitness) Peek(seriesKey string) lease.State {
	st, err := w.client.LeasePeek(context.Background(), w.nodeID, seriesKey)
	if err != nil {
		return lease.State{}
	}
	return st
}

// Acquire commits the lease on the remote witness via fencing-guarded POST.
func (w *RemoteWitness) Acquire(seriesKey string, holder string, version int, ttlMS int64, token lease.Token) (lease.State, error) {
	return w.client.LeaseAcquire(context.Background(), w.nodeID, lease.AcquireRequest{
		Series:  seriesKey,
		Holder:  holder,
		Version: version,
		TTLMS:   ttlMS,
		Token:   token,
	})
}

// LeasePeek fetches the committed lease state for a series from a witness node.
func (c *Client) LeasePeek(ctx context.Context, nodeID, seriesKey string) (lease.State, error) {
	peerURL, ok := c.membership.PeerURL(nodeID)
	if !ok {
		return lease.State{}, apperror.New(apperror.CodeUnknownNode, "witness has no peer url", 0)
	}
	callCtx, cancel := context.WithTimeout(ctx, c.http.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, peerURL+"/internal/v1/lease/peek?series="+url.QueryEscape(seriesKey), nil)
	if err != nil {
		return lease.State{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return lease.State{}, apperror.New(apperror.CodeLeaseUnavailable, "lease peek failed", 0)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return lease.State{}, apperror.New(apperror.CodeLeaseUnavailable, "lease peek rejected", 0)
	}
	var env lease.StateEnvelope
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&env); err != nil {
		return lease.State{}, apperror.New(apperror.CodeLeaseUnavailable, "lease peek unreadable", 0)
	}
	return env.ToState(), nil
}

// LeaseAcquire commits a lease on a witness node and returns the committed state.
func (c *Client) LeaseAcquire(ctx context.Context, nodeID string, req lease.AcquireRequest) (lease.State, error) {
	peerURL, ok := c.membership.PeerURL(nodeID)
	if !ok {
		return lease.State{}, apperror.New(apperror.CodeUnknownNode, "witness has no peer url", 0)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return lease.State{}, err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.http.Timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, peerURL+"/internal/v1/lease/acquire", bytes.NewReader(body))
	if err != nil {
		return lease.State{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return lease.State{}, apperror.New(apperror.CodeLeaseUnavailable, "lease acquire transport failed", 0)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return lease.State{}, decodeLeaseError(resp.Body)
	}
	var env lease.StateEnvelope
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&env); err != nil {
		return lease.State{}, apperror.New(apperror.CodeLeaseUnavailable, "lease acquire unreadable", 0)
	}
	return env.ToState(), nil
}

func decodeLeaseError(body io.Reader) error {
	var decoded peerErrorBody
	raw, err := io.ReadAll(io.LimitReader(body, 1<<20))
	if err == nil {
		_ = json.Unmarshal(raw, &decoded)
	}
	if decoded.Error == "" {
		return apperror.New(apperror.CodeLeaseUnavailable, "lease witness rejected", 0)
	}
	return apperror.New(apperror.Code(decoded.Error), decoded.Message, 0)
}
