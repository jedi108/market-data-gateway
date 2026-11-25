// Command tbank-recorder is a read-only recorder of T-Bank trades and L2
// order books into hourly gzip JSONL files (issue fin_predict#612).
package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jedi108/market-data-gateway/internal/provider/tbank/gen/investapi"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	depth     = 10 // quant #612: L1–L5 фичи + BBO; 11–20 без гипотезы, вдвое меньше диска
	pingMs    = 30000
	// stallWait trips on a dead transport (no Recv at all, incl. ping echoes); subscription
	// silence while the transport lives is caught by the day1 gap/stall gate, not here (#1066).
	stallWait = 60 * time.Second // 2 missed 30 s pings = silence (#1066)
	maxBack   = time.Minute
	stable    = time.Minute // a session this long resets the backoff, so a flapping stream escalates to ~1 reconnect/min
)

// pingEvery is the client PingRequest cadence (#1066): the sandbox sends no usable
// server-initiated ping stamps while trading, so the probe sends PingRequest{time}
// and measures Recv − the echoed ping_request_time (round-trip path probe).
var pingEvery = 30 * time.Second

var msk = time.FixedZone("MSK", 3*3600)

type instrument struct{ ticker, uid string }

type hourFile struct {
	hour string // YYYY-MM-DD/HH
	f    *os.File
	gz   *gzip.Writer
}

// diskErr marks write failures: the process must exit and let systemd restart it.
type diskErr struct{ error }

type recorder struct {
	dir, sha       string
	instr          []instrument
	now            func() time.Time
	minBack        time.Duration
	files          map[string]*hourFile
	seq            uint64
	flushed        time.Time
	gapFrom        time.Time // start of the unrecorded interval, zero = unknown
	gapWhy         string
	gapSem         string        // gapFrom provenance: "startup" (file mtime/now) or "last_recv" (#1066)
	lastRecv       time.Time     // local clock right after the last successful Recv, before Marshal (#1066)
	lastPing       time.Time     // recv_ts of the last sampled ping record (#1066)
	reconnectDelay time.Duration // actual back+jitter slept before the current session attempt
}

func newRecorder(dir, sha string, instr []instrument) *recorder {
	r := &recorder{dir: dir, sha: sha, instr: instr, now: time.Now, minBack: 2 * time.Second,
		files: map[string]*hourFile{}, gapWhy: "start", gapSem: "startup"}
	// shortcut: start gap begins at the oldest "latest file mtime" across tickers.
	for _, in := range instr {
		m, _ := filepath.Glob(filepath.Join(dir, "tbank", in.ticker, "*", "*.jsonl.gz"))
		if len(m) == 0 {
			continue
		}
		sort.Strings(m)
		if st, err := os.Stat(m[len(m)-1]); err == nil && (r.gapFrom.IsZero() || st.ModTime().Before(r.gapFrom)) {
			r.gapFrom = st.ModTime()
		}
	}
	return r
}

// write appends one JSON line to the ticker's current hour file, rotating on hour change.
func (r *recorder) write(ticker string, line any) error {
	now := r.now()
	hour := now.In(msk).Format("2006-01-02/15")
	h := r.files[ticker]
	if h != nil && h.hour != hour {
		if err := h.close(); err != nil {
			return diskErr{err}
		}
		h = nil
	}
	if h == nil {
		var err error
		if h, err = r.open(ticker, hour, now); err != nil {
			return diskErr{err}
		}
		r.files[ticker] = h
	}
	b, err := json.Marshal(line)
	if err != nil {
		return err
	}
	if _, err = h.gz.Write(append(b, '\n')); err == nil && now.Sub(r.flushed) > time.Second {
		r.flushed = now
		err = h.gz.Flush()
	}
	if err != nil {
		return diskErr{err}
	}
	return nil
}

func (r *recorder) open(ticker, hour string, now time.Time) (*hourFile, error) {
	p := filepath.Join(r.dir, "tbank", ticker, hour+".jsonl.gz")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	// Append: a restart inside the hour adds a new gzip member (valid multi-member file).
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	h := &hourFile{hour: hour, f: f, gz: gzip.NewWriter(f)}
	uid := ""
	for _, in := range r.instr {
		if in.ticker == ticker {
			uid = in.uid
		}
	}
	hdr, _ := json.Marshal(map[string]any{"type": "header", "binary_sha": r.sha, "uid": uid,
		"ticker": ticker, "depth": depth, "started": now.UTC().Format(time.RFC3339Nano),
		"recv_ts_semantics": "local clock immediately after gRPC Recv, before protojson.Marshal (#1066)"})
	if _, err = h.gz.Write(append(hdr, '\n')); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *hourFile) close() error {
	e1 := h.gz.Close()
	if e2 := h.f.Close(); e1 == nil {
		e1 = e2
	}
	return e1
}

func (r *recorder) Close() error {
	var first error
	for t, h := range r.files {
		if err := h.close(); err != nil && first == nil {
			first = err
		}
		delete(r.files, t)
	}
	return first
}

func (r *recorder) writeGap(reason string) error {
	g := map[string]any{"type": "gap", "to": r.now().UTC().Format(time.RFC3339Nano), "reason": reason,
		"from_semantics": r.gapSem, "reconnect_delay": r.reconnectDelay.Milliseconds()}
	if !r.gapFrom.IsZero() {
		g["from"] = r.gapFrom.UTC().Format(time.RFC3339Nano)
	}
	for _, in := range r.instr {
		if err := r.write(in.ticker, g); err != nil {
			return err
		}
	}
	r.gapFrom = time.Time{}
	r.gapSem = ""
	return nil
}

// handle records one stream message. recvTS is the local clock captured right
// after st.Recv returned, before protojson.Marshal: a local CPU stall in this
// goroutine must not be indistinguishable from delivery latency (#1066).
func (r *recorder) handle(m *investapi.MarketDataResponse, recvTS time.Time) error {
	var kind, uid string
	switch p := m.Payload.(type) {
	case *investapi.MarketDataResponse_Trade:
		kind, uid = "trade", p.Trade.InstrumentUid
	case *investapi.MarketDataResponse_Orderbook:
		kind, uid = "orderbook", p.Orderbook.InstrumentUid
	case *investapi.MarketDataResponse_Ping:
		return r.pingProbe(p.Ping, recvTS)
	default:
		log.Printf("stream: %s", protojson.Format(m)) // subscription acks: per-instrument status
		return nil
	}
	for _, in := range r.instr {
		if in.uid == uid {
			raw, err := protojson.Marshal(m)
			if err != nil {
				return err
			}
			r.seq++
			return r.write(in.ticker, map[string]any{"type": kind,
				"recv_ts": recvTS.UTC().Format(time.RFC3339Nano), "seq": r.seq, "msg": json.RawMessage(raw)})
		}
	}
	log.Printf("unknown instrument_uid %s", uid)
	return nil
}

// pingProbe samples the path lag into every instrument file once per ~10 min
// (#1066): pings arrive every 30 s, logging each would bloat the JSONL.
// server_ts_src=ping_request_time: lag_ms is the round-trip of our own
// PingRequest stamp echoed by the server; server_ts_src=time: one-way lag
// against a server-initiated stamp (signed: negative = server ahead of us).
func (r *recorder) pingProbe(p *investapi.Ping, recvTS time.Time) error {
	st := p.GetPingRequestTime()
	src := "ping_request_time"
	if st == nil {
		st = p.GetTime()
		src = "time"
	}
	if st == nil {
		return nil
	}
	if !r.lastPing.IsZero() && recvTS.Sub(r.lastPing) < 10*time.Minute {
		return nil
	}
	r.lastPing = recvTS
	serverTS := st.AsTime()
	rec := map[string]any{"type": "ping",
		"recv_ts":       recvTS.UTC().Format(time.RFC3339Nano),
		"server_ts":     serverTS.UTC().Format(time.RFC3339Nano),
		"server_ts_src": src, // which server stamp lag_ms is measured against (#1066 review D7)
		"lag_ms":        float64(recvTS.Sub(serverTS)) / float64(time.Millisecond)}
	for _, in := range r.instr {
		if err := r.write(in.ticker, rec); err != nil {
			return err
		}
	}
	return nil
}

func (r *recorder) session(ctx context.Context, c investapi.MarketDataStreamServiceClient) (gotData bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	st, err := c.MarketDataStream(ctx)
	if err != nil {
		return false, err
	}
	var ti []*investapi.TradeInstrument
	var oi []*investapi.OrderBookInstrument
	for _, in := range r.instr {
		ti = append(ti, &investapi.TradeInstrument{InstrumentId: in.uid})
		oi = append(oi, &investapi.OrderBookInstrument{InstrumentId: in.uid, Depth: depth})
	}
	ms := int32(pingMs)
	for _, req := range []*investapi.MarketDataRequest{
		{Payload: &investapi.MarketDataRequest_PingSettings{PingSettings: &investapi.PingDelaySettings{PingDelayMs: &ms}}},
		{Payload: &investapi.MarketDataRequest_SubscribeTradesRequest{SubscribeTradesRequest: &investapi.SubscribeTradesRequest{
			SubscriptionAction: investapi.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE, Instruments: ti,
			TradeSource: investapi.TradeSourceType_TRADE_SOURCE_ALL}}},
		{Payload: &investapi.MarketDataRequest_SubscribeOrderBookRequest{SubscribeOrderBookRequest: &investapi.SubscribeOrderBookRequest{
			SubscriptionAction: investapi.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE, Instruments: oi}}},
	} {
		if err = st.Send(req); err != nil {
			return false, err
		}
	}
	// Client-side ping probe (#1066): concurrent Send is safe alongside Recv.
	go func() {
		t := time.NewTicker(pingEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := st.Send(&investapi.MarketDataRequest{Payload: &investapi.MarketDataRequest_Ping{
					Ping: &investapi.PingRequest{Time: timestamppb.New(r.now())}}}); err != nil {
					log.Printf("ping probe: send: %v", err) // stream not necessarily dead; Recv loop decides
					return
				}
			}
		}
	}()
	watchdog := time.AfterFunc(stallWait, cancel)
	defer watchdog.Stop()
	for {
		m, err := st.Recv()
		if err != nil {
			return gotData, err
		}
		// recv_ts semantics (#1066): stamp immediately after Recv, before any
		// Marshal work; lastRecv anchors the next gap at the last received message.
		recvTS := r.now()
		r.lastRecv = recvTS
		watchdog.Reset(stallWait)
		if !gotData {
			gotData = true
			if err := r.writeGap(r.gapWhy); err != nil {
				return true, err
			}
		}
		if err := r.handle(m, recvTS); err != nil {
			return true, err
		}
	}
}

// Run records until ctx is cancelled (nil) or a disk error occurs.
func (r *recorder) Run(ctx context.Context, c investapi.MarketDataStreamServiceClient) error {
	back := r.minBack
	for {
		if r.gapFrom.IsZero() {
			r.gapFrom = r.now()
			r.gapSem = "startup"
		}
		t0 := time.Now()
		got, err := r.session(ctx, c)
		var de diskErr
		if errors.As(err, &de) {
			return de
		}
		if ctx.Err() != nil {
			return nil
		}
		r.gapWhy = fmt.Sprint("stream: ", err)
		log.Printf("%s; reconnect in %s", r.gapWhy, back)
		if got {
			// The outage starts at the last received message, not at the error
			// moment: watchdog silence otherwise shrinks the gap ~13× (#1066).
			r.gapFrom = r.lastRecv
			r.gapSem = "last_recv"
			if time.Since(t0) > stable {
				back = r.minBack
			}
		}
		delay := back + rand.N(back/2+1) // jitter
		r.reconnectDelay = delay
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		if back *= 2; back > maxBack {
			back = maxBack
		}
	}
}
