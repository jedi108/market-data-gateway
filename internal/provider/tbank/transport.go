package tbank

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// PublicEndpoint and SandboxEndpoint are the two documented TBank invest API
// transports. The sandbox endpoint serves the same MarketData surface with a
// sandbox credential; probes and bounded shadows run against it.
const (
	PublicEndpoint  = "invest-public-api.tinkoff.ru:443"
	SandboxEndpoint = "sandbox-invest-public-api.tinkoff.ru:443"
)

// tokenCredential attaches the TBank bearer token to every RPC. The token is
// never logged and never appears in errors produced here.
type tokenCredential struct{ token string }

func (c tokenCredential) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (c tokenCredential) RequireTransportSecurity() bool { return true }

// plaintextTokenCredential is used only when the caller explicitly selected a
// plaintext endpoint (local generated-server harness).
type plaintextTokenCredential struct{ token string }

func (c plaintextTokenCredential) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (c plaintextTokenCredential) RequireTransportSecurity() bool { return false }

// TransportOptions configures the real TBank gRPC transport.
type TransportOptions struct {
	// Endpoint is the explicit host:port. Required: the provider never
	// dials an implicit default.
	Endpoint string
	// Token is the market-data credential (sandbox for probes/shadow).
	Token string
	// CAFile optionally adds a PEM bundle (e.g. the Russian trusted roots)
	// on top of the system roots. Empty means system roots only.
	CAFile string
	// Plaintext allows h2c for local generated-server harnesses. It is
	// rejected for the documented public endpoints.
	Plaintext bool
}

// Validate rejects unsafe transport combinations before any dial.
func (o TransportOptions) Validate() error {
	if o.Endpoint == "" {
		return fmt.Errorf("tbank endpoint is required")
	}
	if o.Token == "" {
		return fmt.Errorf("tbank token is required")
	}
	if o.Plaintext && (o.Endpoint == PublicEndpoint || o.Endpoint == SandboxEndpoint) {
		return fmt.Errorf("plaintext transport is not allowed for TBank public endpoints")
	}
	return nil
}

// DialTransport establishes the gRPC connection for the market data service.
// TLS verification is always on for real endpoints; an extra CA bundle is
// appended to (never replacing) the system roots.
func DialTransport(ctx context.Context, opts TransportOptions) (*grpc.ClientConn, error) {
	if err := opts.Validate(); err != nil {
		return nil, apperror.New(apperror.CodeProviderUnavailable, err.Error(), 0)
	}
	var dialOpts []grpc.DialOption
	if opts.Plaintext {
		dialOpts = append(dialOpts,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(plaintextTokenCredential{opts.Token}))
	} else {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, apperror.New(apperror.CodeProviderUnavailable, "system cert pool unavailable", 0)
		}
		if opts.CAFile != "" {
			pem, readErr := os.ReadFile(opts.CAFile)
			if readErr != nil {
				return nil, apperror.New(apperror.CodeProviderUnavailable, fmt.Sprintf("ca bundle unreadable: %s", opts.CAFile), 0)
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, apperror.New(apperror.CodeProviderUnavailable, fmt.Sprintf("ca bundle contains no certificates: %s", opts.CAFile), 0)
			}
		}
		tlsConfig := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		dialOpts = append(dialOpts,
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			grpc.WithPerRPCCredentials(tokenCredential{opts.Token}))
	}
	conn, err := grpc.DialContext(ctx, opts.Endpoint, dialOpts...)
	if err != nil {
		return nil, apperror.New(apperror.CodeProviderUnavailable, "tbank transport dial failed", 0)
	}
	return conn, nil
}

// RatelimitRemainingFromHeader extracts the documented x-ratelimit remaining
// value from response header metadata. TBank returns x-ratelimit-* values
// like "600, 600;w=60"; only the first comma-separated member is considered.
// It returns -1 when the header is absent or unparseable.
func RatelimitRemainingFromHeader(md map[string][]string) int64 {
	values, ok := md["x-ratelimit-remaining"]
	if !ok || len(values) == 0 {
		return -1
	}
	first := strings.TrimSpace(strings.Split(values[0], ",")[0])
	var remaining int64
	if _, err := fmt.Sscanf(first, "%d", &remaining); err != nil {
		return -1
	}
	return remaining
}
