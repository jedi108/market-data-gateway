// Package capacity contains a pure, deterministic what-if calculator for one
// provider credential group's market-data capacity. It is deliberately not an
// admission controller and has no runtime or provider dependencies.
package capacity

import (
	"errors"
	"math"
	"sort"
	"time"
)

var (
	ErrInvalidInput = errors.New("capacity: invalid input")
	ErrOverflow     = errors.New("capacity: arithmetic overflow or pathological cycle")
)

type Source uint8

const (
	SourceExternal Source = iota
	SourceDerived
)

type GatewayConfig struct {
	SafeBudgetPerMinute int
	TargetUtilization   float64
	RetryFactor         float64
}

type TimeframeLoad struct {
	Period            time.Duration
	Instruments       int
	RequestsPerCandle int
	FreshnessSLA      time.Duration
	Source            Source
}

type Input struct {
	Config    GatewayConfig
	Workloads []TimeframeLoad
}

type BoundaryLoad struct {
	At          time.Duration
	Requests    int
	RequiredRPS float64
}

type Result struct {
	HardRPS          float64
	EffectiveRPS     float64
	AverageRPS       float64
	BurstRPS         float64
	RequiredRequests int
	RequiredGateways int
	MaxDrainTime     time.Duration
	Boundaries       []BoundaryLoad
	Feasible         bool
}

const maxCycle = 365 * 24 * time.Hour
const maxScheduleEvents = 100000

// Check validates and calculates a capacity input. It is kept as a facade so
// callers that only need validation do not depend on implementation details.
func Check(in Input) error { _, err := Calculate(in); return err }

// CanDerive reports whether source is capacity-neutral in this pure what-if
// model. Derived data must already be available; this does not authorize
// aggregation or runtime source switching.
func CanDerive(source Source) bool { return source == SourceDerived }

// MaxInstruments returns the largest instrument count that fits the average
// demand of load at the configured effective rate. It returns zero for a
// derived load, invalid input, or a non-positive rate.
func MaxInstruments(cfg GatewayConfig, load TimeframeLoad) int {
	if load.Source == SourceDerived || load.Period <= 0 || load.RequestsPerCandle <= 0 || cfg.SafeBudgetPerMinute <= 0 || cfg.TargetUtilization <= 0 || cfg.RetryFactor <= 0 {
		return 0
	}
	rate := float64(cfg.SafeBudgetPerMinute) / 60 * cfg.TargetUtilization
	return int(math.Floor(rate * load.Period.Seconds() / (float64(load.RequestsPerCandle) * cfg.RetryFactor)))
}

func Calculate(in Input) (Result, error) {
	cfg := in.Config
	if cfg.SafeBudgetPerMinute <= 0 || cfg.TargetUtilization <= 0 || cfg.TargetUtilization > 1 || cfg.RetryFactor < 1 || math.IsNaN(cfg.TargetUtilization) || math.IsInf(cfg.TargetUtilization, 0) || math.IsNaN(cfg.RetryFactor) || math.IsInf(cfg.RetryFactor, 0) {
		return Result{}, ErrInvalidInput
	}
	r := Result{HardRPS: float64(cfg.SafeBudgetPerMinute) / 60, EffectiveRPS: float64(cfg.SafeBudgetPerMinute) / 60 * cfg.TargetUtilization, Feasible: true}
	var cycle time.Duration = time.Second
	for _, load := range in.Workloads {
		if load.Period <= 0 || load.Instruments < 0 || load.RequestsPerCandle < 0 || load.FreshnessSLA <= 0 || load.Source > SourceDerived {
			return Result{}, ErrInvalidInput
		}
		if load.Source == SourceDerived || load.Instruments == 0 || load.RequestsPerCandle == 0 {
			continue
		}
		n, ok := checkedMul(load.Instruments, load.RequestsPerCandle)
		if !ok {
			return Result{}, ErrOverflow
		}
		if !okDurationLCM(cycle, load.Period) {
			return Result{}, ErrOverflow
		}
		cycle = lcm(cycle, load.Period)
		if cycle > maxCycle {
			return Result{}, ErrOverflow
		}
		if r.RequiredRequests, ok = checkedAdd(r.RequiredRequests, n); !ok {
			return Result{}, ErrOverflow
		}
		r.AverageRPS += float64(n) / load.Period.Seconds() * cfg.RetryFactor
		drain := time.Duration(float64(n) / r.EffectiveRPS * float64(time.Second))
		if drain > r.MaxDrainTime {
			r.MaxDrainTime = drain
		}
	}
	if len(in.Workloads) == 0 || r.RequiredRequests == 0 {
		return r, nil
	}
	type boundary struct {
		requests int
		rps      float64
	}
	byAt := make(map[time.Duration]boundary)
	events := 0
	for _, load := range in.Workloads {
		if load.Source != SourceExternal || load.Instruments == 0 || load.RequestsPerCandle == 0 {
			continue
		}
		n, _ := checkedMul(load.Instruments, load.RequestsPerCandle)
		for at := time.Duration(0); ; {
			events++
			if events > maxScheduleEvents {
				return Result{}, ErrOverflow
			}
			entry := byAt[at]
			var ok bool
			entry.requests, ok = checkedAdd(entry.requests, n)
			if !ok {
				return Result{}, ErrOverflow
			}
			entry.rps += float64(n) / load.FreshnessSLA.Seconds()
			byAt[at] = entry
			if at > cycle-load.Period {
				break
			}
			at += load.Period
		}
	}
	for at, entry := range byAt {
		r.Boundaries = append(r.Boundaries, BoundaryLoad{At: at, Requests: entry.requests, RequiredRPS: entry.rps})
	}
	sort.Slice(r.Boundaries, func(i, j int) bool { return r.Boundaries[i].At < r.Boundaries[j].At })
	for _, b := range r.Boundaries {
		if b.RequiredRPS > r.BurstRPS {
			r.BurstRPS = b.RequiredRPS
		}
		if b.RequiredRPS > r.EffectiveRPS {
			r.Feasible = false
		}
	}
	required := r.AverageRPS / r.EffectiveRPS
	if burstRequired := r.BurstRPS / r.EffectiveRPS; burstRequired > required {
		required = burstRequired
	}
	r.RequiredGateways = int(math.Ceil(required))
	if r.RequiredGateways < 1 {
		r.RequiredGateways = 1
	}
	if r.RequiredGateways == 1 && !r.Feasible {
		r.RequiredGateways = 2
	}
	return r, nil
}

func checkedMul(a, b int) (int, bool) {
	if a < 0 || b < 0 || (b != 0 && a > math.MaxInt/b) {
		return 0, false
	}
	return a * b, true
}
func checkedAdd(a, b int) (int, bool) {
	if b > 0 && a > math.MaxInt-b {
		return 0, false
	}
	return a + b, true
}
func gcd(a, b time.Duration) time.Duration {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
func okDurationLCM(a, b time.Duration) bool { g := gcd(a, b); return g != 0 && a/g <= maxCycle/b }
func lcm(a, b time.Duration) time.Duration  { return a / gcd(a, b) * b }
