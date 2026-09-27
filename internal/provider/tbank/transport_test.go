package tbank

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/jedi108/market-data-gateway/internal/model"
	investapi "github.com/jedi108/market-data-gateway/internal/provider/tbank/gen/investapi"
)

func TestTransportOptionsValidate(t *testing.T) {
	cases := []struct {
		name    string
		opts    TransportOptions
		wantErr bool
	}{
		{"missing endpoint", TransportOptions{Endpoint: "", Token: "t"}, true},
		{"missing token", TransportOptions{Endpoint: SandboxEndpoint, Token: ""}, true},
		{"plaintext public endpoint rejected", TransportOptions{Endpoint: PublicEndpoint, Token: "t", Plaintext: true}, true},
		{"plaintext sandbox endpoint rejected", TransportOptions{Endpoint: SandboxEndpoint, Token: "t", Plaintext: true}, true},
		{"valid tls sandbox", TransportOptions{Endpoint: SandboxEndpoint, Token: "t"}, false},
		{"valid plaintext local harness", TransportOptions{Endpoint: "127.0.0.1:59999", Token: "t", Plaintext: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if (err != nil) && !tc.wantErr {
				t.Fatalf("unexpected error: %v", err)
			}
			if err == nil && tc.wantErr {
				t.Fatalf("expected error, got none")
			}
		})
	}
}

func TestRatelimitRemainingFromHeader(t *testing.T) {
	cases := []struct {
		name string
		md   map[string][]string
		want int64
	}{
		{"absent", map[string][]string{}, -1},
		{"empty values", map[string][]string{"x-ratelimit-remaining": {}}, -1},
		{"simple integer", map[string][]string{"x-ratelimit-remaining": {"596"}}, 596},
		{"comma list takes first", map[string][]string{"x-ratelimit-remaining": {"600, 600;w=60"}}, 600},
		{"unparseable", map[string][]string{"x-ratelimit-remaining": {"unlimited"}}, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RatelimitRemainingFromHeader(tc.md); got != tc.want {
				t.Fatalf("RatelimitRemainingFromHeader(%v) = %d, want %d", tc.md, got, tc.want)
			}
		})
	}
}

// headerEchoServer serves the MarketDataService surface and echoes incoming
// authorization metadata back as response headers so tests can assert the
// bearer token credential is attached to every RPC.
type headerEchoServer struct {
	investapi.UnimplementedMarketDataServiceServer
	t *testing.T
}

func (s *headerEchoServer) GetCandles(ctx context.Context, req *investapi.GetCandlesRequest) (*investapi.GetCandlesResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		s.t.Errorf("no incoming metadata")
	}
	auth := md.Get("authorization")
	if len(auth) != 1 {
		s.t.Errorf("authorization metadata missing, got %v", auth)
	} else if auth[0] != "Bearer test-token" {
		s.t.Errorf("authorization metadata wrong value")
	}
	_ = grpc.SendHeader(ctx, metadata.Pairs("x-ratelimit-remaining", "596"))
	return &investapi.GetCandlesResponse{}, nil
}

func TestDialTransportPlaintextAttachesTokenAndCapturesHeaders(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	investapi.RegisterMarketDataServiceServer(server, &headerEchoServer{t: t})
	go server.Serve(listener)
	defer server.Stop()

	conn, err := DialTransport(context.Background(), TransportOptions{
		Endpoint:  listener.Addr().String(),
		Token:     "test-token",
		Plaintext: true,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	provider := New(investapi.NewMarketDataServiceClient(conn))
	request := model.CandleRequest{
		Series:    model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "BBG004730N88", Timeframe: "1h", CandleType: "trade"},
		FromUTCMS: 1, ToUTCMS: 2, Limit: 10, IncludeIncomplete: true,
	}
	if err := request.Series.Validate(); err != nil {
		t.Fatalf("series invalid: %v", err)
	}
	_, meta, headers, err := provider.FetchCandlesWithHeaders(context.Background(), request)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if meta.RateLimitRemaining != 596 {
		t.Fatalf("RateLimitRemaining = %d, want 596", meta.RateLimitRemaining)
	}
	if len(headers["X-Ratelimit-Remaining"]) == 0 && len(headers["x-ratelimit-remaining"]) == 0 {
		t.Fatalf("expected x-ratelimit-remaining header in response metadata")
	}
}

func TestDialTransportTLSFailures(t *testing.T) {
	// The TLS path must fail closed: an unreadable or certificate-free CA
	// bundle is a hard error before any dial, and TLS verification stays on
	// for real endpoints (a self-signed local TLS server is rejected).
	dir := t.TempDir()

	unreadable := filepath.Join(dir, "missing.pem")
	if _, err := DialTransport(context.Background(), TransportOptions{
		Endpoint: SandboxEndpoint,
		Token:    "test-token",
		CAFile:   unreadable,
	}); err == nil {
		t.Fatalf("expected error for unreadable CA bundle, got none")
	} else if !strings.Contains(err.Error(), "ca bundle unreadable") {
		t.Fatalf("unexpected error for unreadable CA bundle: %v", err)
	}

	notCerts := filepath.Join(dir, "not-certs.pem")
	if err := os.WriteFile(notCerts, []byte("not a certificate bundle\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := DialTransport(context.Background(), TransportOptions{
		Endpoint: SandboxEndpoint,
		Token:    "test-token",
		CAFile:   notCerts,
	}); err == nil {
		t.Fatalf("expected error for CA bundle without certificates, got none")
	} else if !strings.Contains(err.Error(), "ca bundle contains no certificates") {
		t.Fatalf("unexpected error for certificate-free CA bundle: %v", err)
	}
}

func TestDialTransportTLSVerificationRejectsSelfSignedWithoutCA(t *testing.T) {
	// End-to-end TLS failure behavior: a self-signed server presented over
	// TLS must fail verification when its CA is not in the trust bundle.
	dir := t.TempDir()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mdg-test-selfsigned"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")
	certOut := &bytes.Buffer{}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("pem cert: %v", err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyOut := &bytes.Buffer{}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		t.Fatalf("pem key: %v", err)
	}
	if err := os.WriteFile(certPath, certOut.Bytes(), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyOut.Bytes(), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serverCreds, err := credentials.NewServerTLSFromFile(certPath, keyPath)
	if err != nil {
		t.Fatalf("server tls: %v", err)
	}
	server := grpc.NewServer(grpc.Creds(serverCreds))
	investapi.RegisterMarketDataServiceServer(server, &headerEchoServer{t: t})
	go server.Serve(listener)
	defer server.Stop()

	// No CAFile: the self-signed chain is not in the system trust store, so
	// the TLS handshake must fail when the first RPC is attempted.
	conn, err := DialTransport(context.Background(), TransportOptions{
		Endpoint: listener.Addr().String(),
		Token:    "test-token",
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := investapi.NewMarketDataServiceClient(conn)
	_, _, _, err = New(client).FetchCandlesWithHeaders(ctx, model.CandleRequest{
		Series:    model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "BBG004730N88", Timeframe: "1h", CandleType: "trade"},
		FromUTCMS: 1, ToUTCMS: 2, Limit: 10,
	})
	if err == nil {
		t.Fatalf("expected TLS verification failure against self-signed server without CA, got success")
	}
}
