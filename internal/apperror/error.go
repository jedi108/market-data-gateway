package apperror

import "fmt"

type Code string

const (
	CodeInvalidRequest          Code = "INVALID_REQUEST"
	CodeUnknownSymbol           Code = "UNKNOWN_SYMBOL"
	CodeAmbiguousSymbol         Code = "AMBIGUOUS_SYMBOL"
	CodeProviderUnavailable     Code = "PROVIDER_UNAVAILABLE"
	CodeProviderRateLimited     Code = "PROVIDER_RATE_LIMITED"
	CodeProviderUnauthenticated Code = "PROVIDER_UNAUTHENTICATED"
	// CodeIncompleteCoverage is returned when an upstream response is valid
	// but does not cover the complete requested half-open interval. It is
	// intentionally distinct from provider availability: callers must not treat
	// a partial response as a successful candle window.
	CodeIncompleteCoverage  Code = "INCOMPLETE_COVERAGE"
	CodeNodeUnauthenticated Code = "NODE_UNAUTHENTICATED"
	// CodeUnsupportedCapability rejects a request outside the gateway's fixed
	// capability boundary (F20): the TBank boundary is candles-only, so any
	// input the gateway does not serve (orderbooks, streams, trading RPCs)
	// must fail with this explicit error — never an empty response and never a
	// silent fallback to another source.
	CodeUnsupportedCapability Code = "UNSUPPORTED_CAPABILITY"
	// CodeClientUnauthenticated rejects a client request that fails the
	// configured identity allowlist (task 109): client identity is derived
	// from the matched credential, never from client-controlled headers.
	CodeClientUnauthenticated  Code = "CLIENT_UNAUTHENTICATED"
	CodeClusterMismatch        Code = "CLUSTER_MISMATCH"
	CodeUnknownNode            Code = "UNKNOWN_NODE"
	CodeInvalidHop             Code = "INVALID_HOP"
	CodeRoutingVersionMismatch Code = "ROUTING_VERSION_MISMATCH"
	CodeNotOwner               Code = "NOT_OWNER"
	CodePeerUnavailable        Code = "PEER_UNAVAILABLE"

	// Phase 6 — lease / fencing domain. These codes belong to the automatic
	// failover control plane and never gate the deterministic (manual) path.
	CodeLeaseConflict     Code = "LEASE_CONFLICT"
	CodeLeaseExpired      Code = "LEASE_EXPIRED"
	CodeLeaseUnavailable  Code = "LEASE_UNAVAILABLE"
	CodeLeaseQuorumFailed Code = "LEASE_QUORUM_FAILED"
	CodeLeaseManualMode   Code = "LEASE_MANUAL_MODE"
)

type Error struct {
	Code         Code
	Message      string
	Retryable    bool
	RetryAfterMS int64
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }
func New(code Code, message string, retryAfterMS int64) *Error {
	return &Error{Code: code, Message: message, Retryable: code == CodeProviderRateLimited || code == CodeProviderUnavailable, RetryAfterMS: retryAfterMS}
}
func (e *Error) HTTPStatus() int {
	switch e.Code {
	case CodeInvalidRequest, CodeUnsupportedCapability:
		return 400
	case CodeUnknownSymbol, CodeAmbiguousSymbol:
		return 422
	case CodeProviderRateLimited:
		return 429
	case CodeProviderUnauthenticated:
		return 502
	case CodeIncompleteCoverage:
		return 503
	case CodeNodeUnauthenticated, CodeClusterMismatch, CodeUnknownNode:
		return 403
	case CodeClientUnauthenticated:
		return 401
	case CodeInvalidHop, CodeRoutingVersionMismatch, CodeNotOwner, CodeLeaseConflict:
		return 409
	case CodePeerUnavailable, CodeLeaseUnavailable:
		return 503
	case CodeLeaseExpired, CodeLeaseQuorumFailed, CodeLeaseManualMode:
		return 409
	default:
		return 503
	}
}
