// Command mcp is a read-only Model Context Protocol server for AI agents. It
// is a thin client of the gateway client API (/v1/candles/batch, /v1/health,
// /v1/ready) and never talks to upstream providers, so cache, singleflight
// and the budget guard stay in force.
//
// Transports: stdio (default, local agents) and Streamable HTTP (remote
// agents, endpoint /mcp) — HTTP requires a bearer token from GATEWAY_MCP_TOKEN and a literal
// loopback or private listen address (fail-closed, like the peer boundary).
package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/jedi108/market-data-gateway/internal/config"
)

var build = "dev"

const (
	tokenEnv        = "GATEWAY_MCP_TOKEN"         // bearer token of the HTTP transport
	gatewayTokenEnv = "MARKET_DATA_GATEWAY_TOKEN" // optional bearer sent to the gateway (client identity allowlist)
	gatewayURLEnv   = "MARKET_DATA_GATEWAY_URL"
	httpPath        = "/mcp"
)

func main() {
	defaultURL := os.Getenv(gatewayURLEnv)
	if defaultURL == "" {
		defaultURL = "http://127.0.0.1:8088"
	}
	transport := flag.String("transport", "stdio", "MCP transport: stdio or http (Streamable HTTP)")
	listen := flag.String("listen", "127.0.0.1:8090", "HTTP transport listen address: literal loopback or private IP only")
	gatewayURL := flag.String("gateway-url", defaultURL, "gateway client API base URL (default $"+gatewayURLEnv+" or http://127.0.0.1:8088)")
	flag.Parse()
	// stdout belongs to the stdio protocol: logs go to stderr only.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := newServer(newGateway(*gatewayURL, os.Getenv(gatewayTokenEnv)), build)
	var err error
	switch *transport {
	case "stdio":
		err = server.Run(ctx, &mcp.StdioTransport{})
	case "http":
		err = serveHTTP(ctx, server, *listen, os.Getenv(tokenEnv), logger)
	default:
		err = fmt.Errorf("unknown -transport %q (want stdio or http)", *transport)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("mcp server failed", "error", err.Error())
		os.Exit(1)
	}
}

// checkHTTP is the fail-closed start gate of the HTTP transport.
func checkHTTP(listen, token string) error {
	if token == "" {
		return fmt.Errorf("%s is required for -transport http", tokenEnv)
	}
	if !config.NonPublicAddress(listen) {
		return fmt.Errorf("-listen %q must be a literal loopback or private IP address", listen)
	}
	return nil
}

func serveHTTP(ctx context.Context, server *mcp.Server, listen, token string, logger *slog.Logger) error {
	if err := checkHTTP(listen, token); err != nil {
		return err
	}
	httpServer := &http.Server{Addr: listen, Handler: httpHandler(server, token), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdown)
	}()
	logger.Info("mcp http transport listening", "listen", listen, "path", httpPath)
	if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// httpHandler serves Streamable HTTP at /mcp behind the bearer check.
func httpHandler(server *mcp.Server, token string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(httpPath, requireBearer(token, mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)))
	return mux
}

// requireBearer rejects requests without the exact bearer token with 401.
// Comparison is constant-time over digests; the token is never logged.
func requireBearer(token string, next http.Handler) http.Handler {
	want := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, value, _ := strings.Cut(r.Header.Get("Authorization"), " ")
		got := sha256.Sum256([]byte(value))
		if !strings.EqualFold(scheme, "Bearer") || value == "" || subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
