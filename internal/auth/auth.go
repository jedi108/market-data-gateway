// Package auth implements the node-credential domain for the private peer
// boundary. It is deliberately separate from any client identity: the peer
// token never authenticates client paths and no client credential ever
// authenticates peer paths.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/jedi108/market-data-gateway/internal/apperror"
)

// NodeAuth verifies the shared cluster node credential presented as
// `Authorization: Bearer <token>`. Comparison is constant-time over fixed
// size digests so neither content nor length leaks through timing.
type NodeAuth struct {
	digest [sha256.Size]byte
	enable bool
}

// NewNodeAuth constructs the verifier. An empty token disables authentication
// entirely — Authenticate then rejects every request, so a misconfigured node
// fails closed instead of opening the peer boundary.
func NewNodeAuth(token string) *NodeAuth {
	auth := &NodeAuth{}
	if token == "" {
		return auth
	}
	auth.digest = sha256.Sum256([]byte(token))
	auth.enable = true
	return auth
}

// Enabled reports whether a node credential is configured.
func (a *NodeAuth) Enabled() bool { return a.enable }

// Authenticate validates the request credential. It returns a typed error
// suitable for the peer HTTP boundary; it never reads or logs the token.
func (a *NodeAuth) Authenticate(r *http.Request) error {
	header := r.Header.Get("Authorization")
	scheme, value, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(value) == "" {
		return apperror.New(apperror.CodeNodeUnauthenticated, "node credential is required", 0)
	}
	if !a.enable {
		return apperror.New(apperror.CodeNodeUnauthenticated, "node authentication is not configured", 0)
	}
	presented := sha256.Sum256([]byte(value))
	if subtle.ConstantTimeCompare(a.digest[:], presented[:]) != 1 {
		return apperror.New(apperror.CodeNodeUnauthenticated, "node credential rejected", 0)
	}
	return nil
}
