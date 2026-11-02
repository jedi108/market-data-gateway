// Package indicators holds pure technical-indicator functions over float64
// price series. Outputs are aligned with the input: warm-up positions that do
// not yet have a value are NaN. Smoothing follows J. Welles Wilder for ATR and
// ADX; EMA is seeded with the simple average of its first period.
package indicators

import "math"

// EMA returns the exponential moving average (k = 2/(period+1)). The first
// value is at index period-1 and equals the SMA of the first period inputs.
func EMA(values []float64, period int) []float64 {
	out := nans(len(values))
	if period < 1 || len(values) < period {
		return out
	}
	sum := 0.0
	for _, v := range values[:period] {
		sum += v
	}
	out[period-1] = sum / float64(period)
	k := 2 / float64(period+1)
	for i := period; i < len(values); i++ {
		out[i] = values[i]*k + out[i-1]*(1-k)
	}
	return out
}

// ATR returns Wilder's average true range. True range starts at index 1 (it
// needs the previous close); the first ATR is at index period and equals the
// mean of the first period true ranges.
func ATR(high, low, close []float64, period int) []float64 {
	n := len(close)
	out := nans(n)
	if period < 1 || n <= period {
		return out
	}
	tr := trueRange(high, low, close)
	sum := 0.0
	for _, v := range tr[1 : period+1] {
		sum += v
	}
	out[period] = sum / float64(period)
	for i := period + 1; i < n; i++ {
		out[i] = (out[i-1]*float64(period-1) + tr[i]) / float64(period)
	}
	return out
}

// ADX returns Wilder's average directional index. DX starts at index period;
// the first ADX is at index 2*period-1 and equals the mean of the first
// period DX values.
func ADX(high, low, close []float64, period int) []float64 {
	n := len(close)
	out := nans(n)
	if period < 1 || n < 2*period {
		return out
	}
	tr := trueRange(high, low, close)
	plusDM, minusDM := make([]float64, n), make([]float64, n)
	for i := 1; i < n; i++ {
		up, down := high[i]-high[i-1], low[i-1]-low[i]
		if up > down && up > 0 {
			plusDM[i] = up
		}
		if down > up && down > 0 {
			minusDM[i] = down
		}
	}
	var str, splus, sminus float64
	for i := 1; i <= period; i++ {
		str, splus, sminus = str+tr[i], splus+plusDM[i], sminus+minusDM[i]
	}
	p := float64(period)
	dx := make([]float64, n)
	dx[period] = directionalIndex(str, splus, sminus)
	for i := period + 1; i < n; i++ {
		str = str - str/p + tr[i]
		splus = splus - splus/p + plusDM[i]
		sminus = sminus - sminus/p + minusDM[i]
		dx[i] = directionalIndex(str, splus, sminus)
	}
	sum := 0.0
	for _, v := range dx[period : 2*period] {
		sum += v
	}
	out[2*period-1] = sum / p
	for i := 2 * period; i < n; i++ {
		out[i] = (out[i-1]*(p-1) + dx[i]) / p
	}
	return out
}

// RealizedVol returns the sample standard deviation (n-1) of log returns of
// close, per bar (not annualized). It is NaN for fewer than three closes.
func RealizedVol(close []float64) float64 {
	if len(close) < 3 {
		return math.NaN()
	}
	returns := make([]float64, len(close)-1)
	mean := 0.0
	for i := 1; i < len(close); i++ {
		returns[i-1] = math.Log(close[i] / close[i-1])
		mean += returns[i-1]
	}
	mean /= float64(len(returns))
	ss := 0.0
	for _, r := range returns {
		ss += (r - mean) * (r - mean)
	}
	return math.Sqrt(ss / float64(len(returns)-1))
}

func trueRange(high, low, close []float64) []float64 {
	tr := make([]float64, len(close))
	tr[0] = math.NaN()
	for i := 1; i < len(close); i++ {
		tr[i] = math.Max(high[i]-low[i], math.Max(math.Abs(high[i]-close[i-1]), math.Abs(low[i]-close[i-1])))
	}
	return tr
}

func directionalIndex(str, splus, sminus float64) float64 {
	if str == 0 {
		return 0
	}
	plusDI, minusDI := 100*splus/str, 100*sminus/str
	if plusDI+minusDI == 0 {
		return 0
	}
	return 100 * math.Abs(plusDI-minusDI) / (plusDI + minusDI)
}

func nans(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = math.NaN()
	}
	return out
}
