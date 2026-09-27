// Client identity allowlist for the loopback client boundary (task 109).
// Identity and scheduler priority are derived from the matched credential —
// never from client-controlled headers — so a caller cannot raise its own
// priority or impersonate another client's quota. Token values are read from
// the protected host environment only; the config references env names.
package auth

import (
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/config"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
)

// ClientIdentity is one resolved allowlist entry.
type ClientIdentity struct {
	ID       string
	Priority ratelimit.Priority
}

// ClientDirectory resolves requests to logical client identities. When no
// identities are configured (or none could be resolved from the environment),
// Enabled() is false and the boundary falls back to the legacy loopback
// behavior; a partially configured directory fails closed for unmatched
// credentials.
type ClientDirectory struct {
	enabled atomic.Bool
	byToken map[string]ClientIdentity
}

// NewClientDirectory builds the directory from the validated config entries.
// Entries whose credential env is unset on this host are skipped with a
// warning — that identity simply cannot authenticate until the operator
// provisions its secret.
func NewClientDirectory(defs []config.ClientIdentity, getenv func(string) string, warn func(msg string, args ...any)) *ClientDirectory {
	d := &ClientDirectory{byToken: make(map[string]ClientIdentity)}
	configured := false
	for _, def := range defs {
		token := strings.TrimSpace(getenv(def.TokenEnv))
		if token == "" {
			if warn != nil {
				warn("client identity disabled: credential env is not set on this host", "client_id", def.ID, "token_env", def.TokenEnv)
			}
			continue
		}
		configured = true
		priority := ratelimit.PriorityLiveRefresh
		if def.Priority == "warmup" {
			priority = ratelimit.PriorityWarmup
		}
		d.byToken[token] = ClientIdentity{ID: def.ID, Priority: priority}
	}
	d.enabled.Store(configured)
	return d
}

// Enabled reports whether the allowlist is active. When false, the HTTP
// boundary keeps the legacy behavior (advisory X-Client-ID, shared
// live-refresh priority).
func (d *ClientDirectory) Enabled() bool { return d != nil && d.enabled.Load() }

// Resolve maps a request to its logical client identity and fixed priority.
// It returns a typed 401 error for missing or unknown credentials — the
// rejection happens before admission, so an unauthenticated caller can never
// consume scheduler capacity.
func (d *ClientDirectory) Resolve(r *http.Request) (string, ratelimit.Priority, error) {
	header := r.Header.Get("Authorization")
	scheme, value, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(value) == "" {
		return "", 0, apperror.New(apperror.CodeClientUnauthenticated, "client credential is required", 0)
	}
	identity, known := d.byToken[value]
	if !known {
		return "", 0, apperror.New(apperror.CodeClientUnauthenticated, "client credential rejected", 0)
	}
	return identity.ID, identity.Priority, nil
}
