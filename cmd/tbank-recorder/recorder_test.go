package main

import (
	"compress/gzip"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/provider/tbank/gen/investapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fake struct {
	investapi.UnimplementedMarketDataStreamServiceServer
	calls atomic.Int32
}

func (f *fake) MarketDataStream(s grpc.BidiStreamingServer[investapi.MarketDataRequest, investapi.MarketDataResponse]) error {
	for i := 0; i < 3; i++ { // consume ping+subscriptions so the client's Sends never hit a closed stream
		if _, err := s.Recv(); err != nil {
			return err
		}
	}
	_ = s.Send(&investapi.MarketDataResponse{Payload: &investapi.MarketDataResponse_Trade{
		Trade: &investapi.Trade{InstrumentUid: "uid1", Quantity: 3}}})
	if f.calls.Add(1) == 1 {
		return status.Error(14, "boom") // drop the stream mid-flight
	}
	<-s.Context().Done()
	return nil
}

func readGz(t *testing.T, p string) string {
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(z) // fails on a truncated stream
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestReconnectWritesGap(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	f := &fake{}
	investapi.RegisterMarketDataStreamServiceServer(srv, f)
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := grpc.NewClient("passthrough:///x", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	dir := t.TempDir()
	r := newRecorder(dir, "sha", []instrument{{"SiZ6", "uid1"}})
	r.minBack = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- r.Run(ctx, investapi.NewMarketDataStreamServiceClient(conn)) }()
	for f.calls.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	m, _ := filepath.Glob(filepath.Join(dir, "tbank/SiZ6/*/*.jsonl.gz"))
	if len(m) != 1 {
		t.Fatalf("files: %v", m)
	}
	out := readGz(t, m[0])
	if strings.Count(out, `"type":"trade"`) != 2 || strings.Count(out, `"type":"gap"`) != 2 ||
		!strings.Contains(out, `"reason":"stream:`) || !strings.Contains(strings.SplitN(out, "\n", 2)[0], `"type":"header"`) {
		t.Fatalf("unexpected output:\n%s", out)
	}
}

func TestHourRotation(t *testing.T) {
	dir := t.TempDir()
	r := newRecorder(dir, "sha", []instrument{{"SiZ6", "uid1"}})
	clock := time.Date(2026, 12, 1, 10, 59, 0, 0, msk)
	r.now = func() time.Time { return clock }
	if err := r.write("SiZ6", map[string]any{"type": "trade", "n": 1}); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Minute)
	if err := r.write("SiZ6", map[string]any{"type": "trade", "n": 2}); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(dir, "tbank/SiZ6/2026-12-01/10.jsonl.gz")
	// previous hour must be fully readable while the recorder is still running
	if out := readGz(t, first); !strings.Contains(out, `"n":1`) || strings.Contains(out, `"n":2`) {
		t.Fatalf("hour 10: %s", out)
	}
	r.Close()
	if out := readGz(t, filepath.Join(dir, "tbank/SiZ6/2026-12-01/11.jsonl.gz")); !strings.Contains(out, `"n":2`) {
		t.Fatalf("hour 11: %s", out)
	}
}
