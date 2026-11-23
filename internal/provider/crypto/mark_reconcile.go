package crypto

import (
	"math/big"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// ReconcileMark compares independently read archive and API windows. The caller
// must establish both sources were fully read and the archive pin verified.
// Agreement is diagnostic only: both sources may omit the same candle.
func ReconcileMark(archive, api []model.Candle, fromMS, toMS int64, sourcesRead bool) ([]model.Candle, error) {
	fail := func() ([]model.Candle, error) {
		return nil, apperror.New(apperror.CodeCoverageUnknown, "mark sources do not reconcile", 0)
	}
	if !sourcesRead || fromMS < 0 || toMS <= fromMS || len(archive) == 0 || len(archive) != len(api) {
		return fail()
	}
	previousEnd := int64(-1)
	for i, a := range archive {
		b := api[i]
		if a.Validate() != nil || b.Validate() != nil || !a.IsClosed || !b.IsClosed || a.OpenTimeUTCMS < fromMS || a.CloseTimeUTCMS > toMS || a.OpenTimeUTCMS < previousEnd || a.OpenTimeUTCMS != b.OpenTimeUTCMS || a.CloseTimeUTCMS != b.CloseTimeUTCMS {
			return fail()
		}
		av, _ := new(big.Rat).SetString(a.Volume)
		bv, _ := new(big.Rat).SetString(b.Volume)
		if av.Sign() != 0 || bv.Sign() != 0 {
			return fail()
		}
		for j, x := range []string{a.Open, a.High, a.Low, a.Close} {
			y := []string{b.Open, b.High, b.Low, b.Close}[j]
			if !fundingRatePattern.MatchString(x) || !fundingRatePattern.MatchString(y) {
				return fail()
			}
			xr, _ := new(big.Rat).SetString(x)
			yr, _ := new(big.Rat).SetString(y)
			if xr.Sign() <= 0 || yr.Sign() <= 0 || xr.Cmp(yr) != 0 {
				return fail()
			}
		}
		previousEnd = a.CloseTimeUTCMS
	}
	return append([]model.Candle(nil), archive...), nil
}
