package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jedi108/market-data-gateway/internal/provider/tbank"
	"github.com/jedi108/market-data-gateway/internal/provider/tbank/gen/investapi"
)

func main() {
	var instr []instrument
	for _, kv := range strings.Split(os.Getenv("RECORDER_INSTRUMENTS"), ",") {
		if t, u, ok := strings.Cut(strings.TrimSpace(kv), "="); ok && t != "" && u != "" {
			instr = append(instr, instrument{t, u})
		}
	}
	dir := os.Getenv("RECORDER_OUT_DIR")
	if dir == "" || len(instr) == 0 {
		log.Fatal("RECORDER_OUT_DIR and RECORDER_INSTRUMENTS (TICKER=UID,...) are required")
	}
	sha := ""
	if exe, err := os.Executable(); err == nil {
		if b, err := os.ReadFile(exe); err == nil {
			s := sha256.Sum256(b)
			sha = hex.EncodeToString(s[:])
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	// Our TBANK_TOKEN is a sandbox token; sandbox streams real market data (checked vs ISS, #612).
	ep := os.Getenv("TBANK_ENDPOINT")
	if ep == "" {
		ep = tbank.SandboxEndpoint
	}
	conn, err := tbank.DialTransport(ctx, tbank.TransportOptions{Endpoint: ep,
		Token: os.Getenv("TBANK_TOKEN"), CAFile: os.Getenv("TINKOFF_GRPC_ROOT_CERTS_FILE")})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	r := newRecorder(dir, sha, instr)
	err = r.Run(ctx, investapi.NewMarketDataStreamServiceClient(conn))
	if cerr := r.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		log.Fatal(err)
	}
}
