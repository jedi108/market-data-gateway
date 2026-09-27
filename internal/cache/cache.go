package cache

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/jedi108/market-data-gateway/internal/model"
)

type Entry struct {
	Candles []model.Candle
	Replica bool
}

// Range is a half-open UTC millisecond interval. Coverage is separate from
// candles because an absent candle must not be mistaken for a fetched interval.
type Range struct{ FromUTCMS, ToUTCMS int64 }

func (r Range) Validate() error {
	if r.FromUTCMS < 0 || r.ToUTCMS <= r.FromUTCMS {
		return fmt.Errorf("invalid coverage range [%d,%d)", r.FromUTCMS, r.ToUTCMS)
	}
	return nil
}

// LookupResult distinguishes a full hit from a partial hit. Missing contains
// normalized uncovered portions of the requested interval.
type LookupResult struct {
	Entry    Entry
	Missing  []Range
	Complete bool
}

type Metrics struct{ Hits, Misses, Coalesced uint64 }

type storedEntry struct {
	Entry
	Coverage []Range
}

type flight struct {
	done chan struct{}
	err  error
}

type Cache struct {
	mu        sync.RWMutex
	entries   map[string]storedEntry
	flights   map[string]*flight
	hits      atomic.Uint64
	misses    atomic.Uint64
	coalesced atomic.Uint64
}

func New() *Cache {
	return &Cache{entries: make(map[string]storedEntry), flights: make(map[string]*flight)}
}

// Merge preserves the original API and records only the candle intervals it
// received. Call MergeCoverage when an upstream response explicitly proves a
// larger contiguous interval.
func (c *Cache) Merge(key model.SeriesKey, candles []model.Candle, replica bool) error {
	if len(candles) == 0 {
		return nil
	}
	coverage := make([]Range, 0, len(candles))
	for _, candle := range candles {
		coverage = append(coverage, Range{FromUTCMS: candle.OpenTimeUTCMS, ToUTCMS: candle.CloseTimeUTCMS})
	}
	return c.MergeCoverage(key, candles, coverage, replica)
}

// MergeCoverage atomically merges validated candles and explicitly proven
// coverage. It never marks an interval covered merely because it was requested.
func (c *Cache) MergeCoverage(key model.SeriesKey, candles []model.Candle, coverage []Range, replica bool) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if len(candles) == 0 && len(coverage) == 0 {
		return nil
	}
	for _, covered := range coverage {
		if err := covered.Validate(); err != nil {
			return err
		}
	}
	byOpen := make(map[int64]model.Candle)
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.entries[key.Identity()]
	for _, candle := range old.Candles {
		byOpen[candle.OpenTimeUTCMS] = candle
	}
	for _, candle := range candles {
		if err := candle.Validate(); err != nil {
			return fmt.Errorf("invalid candle: %w", err)
		}
		if prior, found := byOpen[candle.OpenTimeUTCMS]; found && prior.IsClosed && !candle.IsClosed {
			continue
		}
		byOpen[candle.OpenTimeUTCMS] = candle
	}
	merged := make([]model.Candle, 0, len(byOpen))
	for _, candle := range byOpen {
		merged = append(merged, candle)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].OpenTimeUTCMS < merged[j].OpenTimeUTCMS })
	entry := Entry{Candles: merged, Replica: old.Replica && replica}
	if len(old.Candles) == 0 {
		entry.Replica = replica
	}
	c.entries[key.Identity()] = storedEntry{Entry: entry, Coverage: mergeRanges(old.Coverage, coverage)}
	return nil
}

func (c *Cache) Get(key model.SeriesKey, from, to int64) (Entry, bool) {
	result, err := c.Lookup(key, Range{FromUTCMS: from, ToUTCMS: to})
	if err != nil || len(result.Entry.Candles) == 0 {
		return Entry{}, false
	}
	return result.Entry, true
}

func (c *Cache) Lookup(key model.SeriesKey, requested Range) (LookupResult, error) {
	if err := key.Validate(); err != nil {
		return LookupResult{}, err
	}
	if err := requested.Validate(); err != nil {
		return LookupResult{}, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, found := c.entries[key.Identity()]
	if !found {
		c.misses.Add(1)
		return LookupResult{Missing: []Range{requested}}, nil
	}
	out := Entry{Replica: entry.Replica}
	for _, candle := range entry.Candles {
		if candle.OpenTimeUTCMS >= requested.FromUTCMS && candle.CloseTimeUTCMS <= requested.ToUTCMS {
			out.Candles = append(out.Candles, candle)
		}
	}
	missing := missingRanges(requested, entry.Coverage)
	complete := len(missing) == 0
	if complete {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}
	return LookupResult{Entry: out, Missing: missing, Complete: complete}, nil
}

// Do serializes refresh work per SeriesKey without holding a cache lock during
// the callback. A joining waiter's cancellation cannot cancel the shared flight.
func (c *Cache) Do(ctx context.Context, key model.SeriesKey, refresh func() error) (coalesced bool, err error) {
	if err := key.Validate(); err != nil {
		return false, err
	}
	identity := key.Identity()
	c.mu.Lock()
	if active := c.flights[identity]; active != nil {
		c.coalesced.Add(1)
		c.mu.Unlock()
		select {
		case <-active.done:
			return true, active.err
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
	active := &flight{done: make(chan struct{})}
	c.flights[identity] = active
	c.mu.Unlock()

	err = refresh()
	c.mu.Lock()
	active.err = err
	delete(c.flights, identity)
	close(active.done)
	c.mu.Unlock()
	return false, err
}

func (c *Cache) Metrics() Metrics {
	return Metrics{Hits: c.hits.Load(), Misses: c.misses.Load(), Coalesced: c.coalesced.Load()}
}

func mergeRanges(existing, added []Range) []Range {
	ranges := append(append([]Range(nil), existing...), added...)
	if len(ranges) == 0 {
		return nil
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].FromUTCMS < ranges[j].FromUTCMS })
	merged := []Range{ranges[0]}
	for _, current := range ranges[1:] {
		last := &merged[len(merged)-1]
		if current.FromUTCMS <= last.ToUTCMS {
			if current.ToUTCMS > last.ToUTCMS {
				last.ToUTCMS = current.ToUTCMS
			}
			continue
		}
		merged = append(merged, current)
	}
	return merged
}

func missingRanges(requested Range, coverage []Range) []Range {
	missing := make([]Range, 0, len(coverage)+1)
	cursor := requested.FromUTCMS
	for _, covered := range coverage {
		if covered.ToUTCMS <= cursor || covered.FromUTCMS >= requested.ToUTCMS {
			continue
		}
		if covered.FromUTCMS > cursor {
			missing = append(missing, Range{FromUTCMS: cursor, ToUTCMS: min(covered.FromUTCMS, requested.ToUTCMS)})
		}
		if covered.ToUTCMS > cursor {
			cursor = covered.ToUTCMS
		}
		if cursor >= requested.ToUTCMS {
			break
		}
	}
	if cursor < requested.ToUTCMS {
		missing = append(missing, Range{FromUTCMS: cursor, ToUTCMS: requested.ToUTCMS})
	}
	return missing
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
