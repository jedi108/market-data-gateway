package crypto

import (
	"math/big"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// FundingAPIRate is an API observation, without an inferred funding interval.
type FundingAPIRate struct {
	Ts   int64
	Rate string
}

// ReconcileFunding compares a fully read, pinned archive window with a fully
// paginated API window. Matching sources are diagnostic, not completeness proof.
// The caller must establish that both sources were read across [fromMS,toMS).
func ReconcileFunding(archive []model.FundingEvent, api []FundingAPIRate, fromMS, toMS int64, sourcesRead bool) ([]model.FundingEvent, error) {
	fail := func() ([]model.FundingEvent, error) {
		return nil, apperror.New(apperror.CodeCoverageUnknown, "funding sources do not reconcile", 0)
	}
	if !sourcesRead || fromMS < 0 || toMS <= fromMS || len(archive) == 0 || len(archive) != len(api) {
		return fail()
	}
	var previous int64 = -1
	for i, event := range archive {
		observation := api[i]
		a, okA := new(big.Rat).SetString(event.Rate)
		b, okB := new(big.Rat).SetString(observation.Rate)
		if event.Ts < fromMS || event.Ts >= toMS || event.Ts <= previous || observation.Ts != event.Ts || event.IntervalH <= 0 || event.AvailableAt < event.Ts || !okA || !okB || !fundingRatePattern.MatchString(event.Rate) || !fundingRatePattern.MatchString(observation.Rate) || a.Cmp(b) != 0 {
			return fail()
		}
		previous = event.Ts
	}
	return append([]model.FundingEvent(nil), archive...), nil
}
