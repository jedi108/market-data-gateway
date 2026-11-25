package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fake struct {
	investapi.UnimplementedMarketDataStreamServiceServer
	calls atomic.Int32
	tail  time.Duration // silence between the last trade and the stream error
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
		time.Sleep(f.tail)
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
	var gaps []string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, `"type":"gap"`) {
			gaps = append(gaps, line)
		}
	}
	if len(gaps) != 2 || !strings.Contains(gaps[0], `"from_semantics":"startup"`) ||
		!strings.Contains(gaps[1], `"from_semantics":"last_recv"`) ||
		!strings.Contains(gaps[0], `"reconnect_delay":`) || !strings.Contains(gaps[1], `"reconnect_delay":`) {
		t.Fatalf("gap semantics (startup/last_recv, reconnect_delay):\n%s", out)
	}
}

// Regression for #1066: the gap must start at the last received message, not
// at the error moment — this server stays silent 150 ms after the last trade.
func TestGapFromLastRecv(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	f := &fake{tail: 150 * time.Millisecond}
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
	r := newRecorder(dir, "sha", []instrument{{"SiZ6", "uid1"}}) // default real clock
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
	var lastTradeRecv string
	var nGap int
	for _, line := range strings.Split(strings.TrimSpace(readGz(t, m[0])), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		switch rec["type"] {
		case "trade":
			lastTradeRecv = rec["recv_ts"].(string)
		case "gap":
			nGap++
			if nGap == 2 {
				from, _ := rec["from"].(string)
				if from != lastTradeRecv {
					t.Fatalf("second gap from %q, want exactly the last trade recv_ts %q", from, lastTradeRecv)
				}
			}
		}
	}
	if nGap != 2 || lastTradeRecv == "" {
		t.Fatalf("records: gaps=%d, trade recv_ts=%q", nGap, lastTradeRecv)
	}
}

func TestPingProbe(t *testing.T) {
	dir := t.TempDir()
	r := newRecorder(dir, "sha", []instrument{{"SiZ6", "uid1"}, {"RTSZ6", "uid2"}})
	clock := time.Date(2026, 12, 1, 10, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return clock }
	ping := func(server *timestamppb.Timestamp) *investapi.MarketDataResponse {
		return &investapi.MarketDataResponse{Payload: &investapi.MarketDataResponse_Ping{
			Ping: &investapi.Ping{PingRequestTime: server}}}
	}
	// first ping: sampled, positive lag (server stamp behind recv)
	if err := r.handle(ping(timestamppb.New(clock.Add(-250*time.Millisecond))), clock); err != nil {
		t.Fatal(err)
	}
	// 1 min later: inside the ~10 min sample window, not logged
	clock = clock.Add(time.Minute)
	if err := r.handle(ping(timestamppb.New(clock)), clock); err != nil {
		t.Fatal(err)
	}
	// 11 min after the first: sampled again, negative lag (server stamp ahead)
	clock = clock.Add(10 * time.Minute)
	if err := r.handle(ping(timestamppb.New(clock.Add(100*time.Millisecond))), clock); err != nil {
		t.Fatal(err)
	}
	// nil server time: never logged, does not consume the sample slot
	clock = clock.Add(30 * time.Second)
	if err := r.handle(ping(nil), clock); err != nil {
		t.Fatal(err)
	}
	// fallback to Ping.Time when PingRequestTime is absent
	clock = clock.Add(10 * time.Minute)
	fb := &investapi.MarketDataResponse{Payload: &investapi.MarketDataResponse_Ping{
		Ping: &investapi.Ping{Time: timestamppb.New(clock.Add(-50 * time.Millisecond))}}}
	if err := r.handle(fb, clock); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	wantRecv := []string{"2026-12-01T10:00:00Z", "2026-12-01T10:11:00Z", "2026-12-01T10:21:30Z"}
	wantServer := []string{"2026-12-01T09:59:59.75Z", "2026-12-01T10:11:00.1Z", "2026-12-01T10:21:29.95Z"}
	wantLag := []float64{250, -100, 50}
	for _, ticker := range []string{"SiZ6", "RTSZ6"} {
		m, _ := filepath.Glob(filepath.Join(dir, "tbank", ticker, "*/*.jsonl.gz"))
		if len(m) != 1 {
			t.Fatalf("%s files: %v", ticker, m)
		}
		var recvTSs, serverTSs []string
		var lags []float64
		for _, line := range strings.Split(strings.TrimSpace(readGz(t, m[0])), "\n") {
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatal(err)
			}
			if rec["type"] == "ping" {
				recvTSs = append(recvTSs, rec["recv_ts"].(string))
				serverTSs = append(serverTSs, rec["server_ts"].(string))
				lags = append(lags, rec["lag_ms"].(float64))
			}
		}
		if len(lags) != 3 {
			t.Fatalf("%s: ping records = %d, want 3 (sampled ~10 min):\n%s", ticker, len(lags), readGz(t, m[0]))
		}
		for i := range wantLag {
			if lags[i] != wantLag[i] || recvTSs[i] != wantRecv[i] || serverTSs[i] != wantServer[i] {
				t.Fatalf("%s ping[%d] = recv %s server %s lag %v, want recv %s server %s lag %v",
					ticker, i, recvTSs[i], serverTSs[i], lags[i], wantRecv[i], wantServer[i], wantLag[i])
			}
		}
	}
}

func TestStallWait(t *testing.T) {
	if stallWait != 60*time.Second {
		t.Fatalf("stallWait = %s, want 60s: 2 missed 30 s pings = silence (#1066)", stallWait)
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
