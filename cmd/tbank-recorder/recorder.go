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
)

const (
	depth     = 20
	pingMs    = 30000
	stallWait = 2 * time.Minute
	maxBack   = time.Minute
	stable    = time.Minute // a session this long resets the backoff, so a flapping stream escalates to ~1 reconnect/min
)

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
	dir, sha string
	instr    []instrument
	now      func() time.Time
	minBack  time.Duration
	files    map[string]*hourFile
	seq      uint64
	flushed  time.Time
	gapFrom  time.Time // start of the unrecorded interval, zero = unknown
	gapWhy   string
}

func newRecorder(dir, sha string, instr []instrument) *recorder {
	r := &recorder{dir: dir, sha: sha, instr: instr, now: time.Now, minBack: 2 * time.Second,
		files: map[string]*hourFile{}, gapWhy: "start"}
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
		"ticker": ticker, "depth": depth, "started": now.UTC().Format(time.RFC3339Nano)})
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
	g := map[string]any{"type": "gap", "to": r.now().UTC().Format(time.RFC3339Nano), "reason": reason}
	if !r.gapFrom.IsZero() {
		g["from"] = r.gapFrom.UTC().Format(time.RFC3339Nano)
	}
	for _, in := range r.instr {
		if err := r.write(in.ticker, g); err != nil {
			return err
		}
	}
	r.gapFrom = time.Time{}
	return nil
}

func (r *recorder) handle(m *investapi.MarketDataResponse) error {
	var kind, uid string
	switch p := m.Payload.(type) {
	case *investapi.MarketDataResponse_Trade:
		kind, uid = "trade", p.Trade.InstrumentUid
	case *investapi.MarketDataResponse_Orderbook:
		kind, uid = "orderbook", p.Orderbook.InstrumentUid
	case *investapi.MarketDataResponse_Ping:
		return nil
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
				"recv_ts": r.now().UTC().Format(time.RFC3339Nano), "seq": r.seq, "msg": json.RawMessage(raw)})
		}
	}
	log.Printf("unknown instrument_uid %s", uid)
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
	watchdog := time.AfterFunc(stallWait, cancel)
	defer watchdog.Stop()
	for {
		m, err := st.Recv()
		if err != nil {
			return gotData, err
		}
		watchdog.Reset(stallWait)
		if !gotData {
			gotData = true
			if err := r.writeGap(r.gapWhy); err != nil {
				return true, err
			}
		}
		if err := r.handle(m); err != nil {
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
			r.gapFrom = r.now()
			if time.Since(t0) > stable {
				back = r.minBack
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(back + rand.N(back/2+1)): // jitter
		}
		if back *= 2; back > maxBack {
			back = maxBack
		}
	}
}
