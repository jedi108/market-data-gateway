package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

// BatchResult is the per-item outcome of a client batch, in request order.
// Error is nil exactly when the item was served; a failed item never fails
// the rest of the batch. Series echoes the request series for envelope
// labeling.
type BatchResult struct {
	Index  int
	Series model.SeriesKey
	Result Result
	Error  error
}

// BatchOptions bounds the fan-out of one client batch.
type BatchOptions struct {
	// MaxFanOut caps the number of items processed in parallel across the
	// whole batch (local items and per-owner groups combined).
	MaxFanOut int
}

// defaultBatchFanOut is the bounded parallelism applied when the handler does
// not carry a configured value.
const defaultBatchFanOut = 8

// GetBatch serves a client batch: items are partitioned by deterministic
// owner, local items reuse Get exactly, and per-owner remote groups perform
// one bounded peer sub-batch with per-item results. Failures stay per item
// and never poison the rest of the batch; results keep request order.
func (s *Service) GetBatch(ctx context.Context, clientID string, priority ratelimit.Priority, requests []model.CandleRequest, options BatchOptions) []BatchResult {
	results := make([]BatchResult, len(requests))
	for i := range requests {
		results[i] = BatchResult{Index: i, Series: requests[i].Series}
		if err := requests[i].Series.Validate(); err != nil {
			results[i].Error = err
		} else if requests[i].FromUTCMS < 0 || requests[i].ToUTCMS <= requests[i].FromUTCMS || requests[i].Limit <= 0 {
			results[i].Error = apperror.New(apperror.CodeInvalidRequest, "batch item window or limit is invalid", 0)
		}
	}

	fanOut := options.MaxFanOut
	if fanOut <= 0 {
		fanOut = defaultBatchFanOut
	}

	// Partition by owner: local items run individually through Get; remote
	// items group per owner node for one bounded sub-batch hop each.
	localIdx := make([]int, 0, len(requests))
	remoteByOwner := make(map[string][]int)
	for i, request := range requests {
		if results[i].Error != nil {
			continue
		}
		if s.cfg.Router == nil {
			localIdx = append(localIdx, i)
			continue
		}
		local, err := s.cfg.Router.IsLocalOwner(request.Series)
		if err != nil {
			results[i].Error = err
			continue
		}
		if local {
			localIdx = append(localIdx, i)
			continue
		}
		owner, err := s.cfg.Router.OwnerOf(request.Series)
		if err != nil {
			results[i].Error = err
			continue
		}
		remoteByOwner[owner] = append(remoteByOwner[owner], i)
	}

	tokens := make(chan struct{}, fanOut)
	var wg sync.WaitGroup
	for _, i := range localIdx {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			tokens <- struct{}{}
			defer func() { <-tokens }()
			result, err := s.Get(ctx, clientID, priority, requests[index])
			results[index].Result = result
			results[index].Error = err
		}(i)
	}
	for owner, indexes := range remoteByOwner {
		wg.Add(1)
		go func(ownerID string, owned []int) {
			defer wg.Done()
			tokens <- struct{}{}
			defer func() { <-tokens }()
			s.peerBatchGroup(ctx, requests, owned, results)
		}(owner, indexes)
	}
	wg.Wait()
	s.logInfo("client batch served", model.SeriesKey{}, "items", len(requests), "local", len(localIdx), "remote_groups", len(remoteByOwner))
	return results
}

// peerBatchGroup serves one owner's slice of the batch through a single
// bounded peer sub-batch request. Item-level failures from the owner stay
// per-item; a transport-level failure marks every item in the group with the
// typed error. No path here ever calls the local provider.
func (s *Service) peerBatchGroup(ctx context.Context, requests []model.CandleRequest, indexes []int, results []BatchResult) {
	batch := make([]model.CandleRequest, 0, len(indexes))
	for _, index := range indexes {
		batch = append(batch, requests[index])
	}
	response, err := s.cfg.Peer.PeerBatchFetch(ctx, batch)
	if err != nil {
		s.logWarn("peer batch fetch failed", requests[indexes[0]].Series, "items", len(indexes), "reason", peerFailure(err))
		for _, index := range indexes {
			results[index].Error = err
		}
		return
	}
	for offset, index := range indexes {
		item := response.Items[offset]
		if item.Error != nil {
			results[index].Error = item.Error
			continue
		}
		result, err := s.commitPeerBatchItem(requests[index], item.Result.Candles, item.Result.SourceFetchedAtUTCMS)
		if err != nil {
			results[index].Error = err
			continue
		}
		result.Replica = true
		results[index].Result = result
	}
}

// commitPeerBatchItem validates and commits one peer-batch item's candles as
// a read-only local replica, then re-reads coverage exactly like peerFetch.
func (s *Service) commitPeerBatchItem(request model.CandleRequest, candles []model.Candle, fetchedAtUTCMS int64) (Result, error) {
	if len(candles) == 0 {
		return Result{}, ErrIncompleteCoverage
	}
	for index := range candles {
		if err := candles[index].Validate(); err != nil {
			s.recorder().RecordIntegrityRejection(request.Series.Venue, "invalid_peer_ohlcv")
			s.logError("peer batch response integrity rejected", request.Series, "reason", "invalid_peer_ohlcv")
			return Result{}, fmt.Errorf("invalid candle in peer batch response: %w", err)
		}
	}
	coverage := candleCoverage(candles)
	if err := s.store.Save(request.Series, storage.Snapshot{Candles: candles, Metadata: storage.Metadata{Replica: true, SourceFetchedAtUTCMS: fetchedAtUTCMS, Coverage: storageCoverage(coverage)}}); err != nil {
		return Result{}, err
	}
	s.noteFetch(request.Series, fetchedAtUTCMS)
	if err := s.cache.MergeCoverage(request.Series, candles, coverage, true); err != nil {
		return Result{}, err
	}
	s.replicaHits.Add(1)
	memory, err := s.cache.Lookup(request.Series, cacheRangeOf(request))
	if err != nil {
		return Result{}, err
	}
	if !memory.Complete {
		s.recorder().RecordIntegrityRejection(request.Series.Venue, "incomplete_peer_coverage")
		s.logError("peer batch item left coverage incomplete", request.Series, "reason", "incomplete_peer_coverage")
		return Result{}, ErrIncompleteCoverage
	}
	s.recordCandleAge(request.Series, memory.Entry.Candles)
	result := s.memoryResult(request.Series, memory.Entry)
	result.CacheStatus = CacheRefreshed
	return result, nil
}

// cacheRangeOf converts a request window to the cache range type.
func cacheRangeOf(request model.CandleRequest) cache.Range {
	return cache.Range{FromUTCMS: request.FromUTCMS, ToUTCMS: request.ToUTCMS}
}
