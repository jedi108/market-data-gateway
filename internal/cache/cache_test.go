package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/model"
)

func candle(open int64, closed bool) model.Candle {
	return model.Candle{OpenTimeUTCMS: open, CloseTimeUTCMS: open + 60, Open: "1", High: "2", Low: "1", Close: "2", Volume: "0", IsClosed: closed}
}

func TestMergeDeduplicatesAndPreservesClosed(t *testing.T) {
	c := New()
	key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: "1m", CandleType: "trade"}
	if err := c.Merge(key, []model.Candle{candle(60, true), candle(0, true)}, false); err != nil {
		t.Fatal(err)
	}
	if err := c.Merge(key, []model.Candle{candle(60, false), candle(120, false)}, false); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Get(key, 0, 180)
	if !ok || len(got.Candles) != 3 || !got.Candles[1].IsClosed || got.Candles[0].OpenTimeUTCMS != 0 {
		t.Fatalf("unexpected cache result: %+v", got)
	}
}

func TestEmptyMergeDoesNotEraseValidData(t *testing.T) {
	c := New()
	key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: "1m", CandleType: "trade"}
	if err := c.Merge(key, []model.Candle{candle(0, true)}, false); err != nil {
		t.Fatal(err)
	}
	if err := c.Merge(key, nil, false); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Get(key, 0, 60)
	if !ok || len(got.Candles) != 1 {
		t.Fatal("valid cache was erased")
	}
}

func TestLookupReportsPartialCoverageAndMergeFillsOnlyGap(t *testing.T) {
	c := New()
	key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: "1m", CandleType: "trade"}
	if err := c.MergeCoverage(key, []model.Candle{candle(0, true)}, []Range{{FromUTCMS: 0, ToUTCMS: 60}}, false); err != nil {
		t.Fatal(err)
	}
	partial, err := c.Lookup(key, Range{FromUTCMS: 0, ToUTCMS: 180})
	if err != nil {
		t.Fatal(err)
	}
	if partial.Complete || len(partial.Missing) != 1 || partial.Missing[0] != (Range{FromUTCMS: 60, ToUTCMS: 180}) {
		t.Fatalf("partial lookup = %+v", partial)
	}
	if err := c.MergeCoverage(key, []model.Candle{candle(60, true), candle(120, true)}, []Range{{FromUTCMS: 60, ToUTCMS: 180}}, false); err != nil {
		t.Fatal(err)
	}
	complete, err := c.Lookup(key, Range{FromUTCMS: 0, ToUTCMS: 180})
	if err != nil || !complete.Complete || len(complete.Entry.Candles) != 3 {
		t.Fatalf("complete lookup = %+v, err=%v", complete, err)
	}
	metrics := c.Metrics()
	if metrics.Hits != 1 || metrics.Misses != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestEmptyMergeDoesNotClaimRequestedCoverage(t *testing.T) {
	c := New()
	key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: "1m", CandleType: "trade"}
	if err := c.Merge(key, nil, false); err != nil {
		t.Fatal(err)
	}
	got, err := c.Lookup(key, Range{FromUTCMS: 0, ToUTCMS: 60})
	if err != nil || got.Complete || len(got.Missing) != 1 {
		t.Fatalf("empty merge lookup = %+v, err=%v", got, err)
	}
}

func TestSingleflightCoalescesAndWaiterCancellationDoesNotCancelRefresh(t *testing.T) {
	c := New()
	key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: "1m", CandleType: "trade"}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	ownerDone := make(chan error, 1)
	go func() {
		_, err := c.Do(context.Background(), key, func() error {
			calls.Add(1)
			close(started)
			<-release
			return nil
		})
		ownerDone <- err
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	joined := make(chan error, 1)
	go func() {
		coalesced, err := c.Do(ctx, key, func() error { return errors.New("joined refresh must not run") })
		if !coalesced {
			joined <- errors.New("request did not join active flight")
			return
		}
		joined <- err
	}()
	cancel()
	if err := <-joined; !errors.Is(err, context.Canceled) {
		t.Fatalf("joined waiter error = %v", err)
	}
	close(release)
	if err := <-ownerDone; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || c.Metrics().Coalesced != 1 {
		t.Fatalf("calls=%d metrics=%+v", calls.Load(), c.Metrics())
	}
}

func TestSingleflightOneRefreshForConcurrentSameSeries(t *testing.T) {
	c := New()
	key := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: "1m", CandleType: "trade"}
	var calls atomic.Int32
	const requests = 100
	started := make(chan struct{})
	release := make(chan struct{})
	ownerDone := make(chan error, 1)
	go func() {
		_, err := c.Do(context.Background(), key, func() error {
			calls.Add(1)
			close(started)
			<-release
			return nil
		})
		ownerDone <- err
	}()
	<-started
	var waiters sync.WaitGroup
	errs := make(chan error, requests-1)
	for range requests - 1 {
		waiters.Add(1)
		go func() {
			defer waiters.Done()
			coalesced, err := c.Do(context.Background(), key, func() error { return errors.New("joined refresh must not run") })
			if !coalesced {
				errs <- errors.New("request did not join active flight")
				return
			}
			errs <- err
		}()
	}
	deadline := time.Now().Add(time.Second)
	for c.Metrics().Coalesced != requests-1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.Metrics().Coalesced != requests-1 {
		t.Fatalf("coalesced = %d, want %d", c.Metrics().Coalesced, requests-1)
	}
	close(release)
	if err := <-ownerDone; err != nil {
		t.Fatal(err)
	}
	waiters.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("refresh calls = %d, want 1", calls.Load())
	}
}
