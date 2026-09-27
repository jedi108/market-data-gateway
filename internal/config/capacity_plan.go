package config

import (
	"fmt"
	"math"
	"time"

	"github.com/jedi108/market-data-gateway/internal/capacity"
)

// CapacityPlan is a startup-scoped forecast. It is never consulted by the
// request scheduler or provider path.
type CapacityPlan struct {
	Mode              string                 `yaml:"mode"`
	CredentialGroup   string                 `yaml:"credential_group"`
	GatewayCount      int                    `yaml:"gateway_count"`
	TargetUtilization float64                `yaml:"target_utilization"`
	RetryFactor       float64                `yaml:"retry_factor"`
	Workloads         []CapacityPlanWorkload `yaml:"workloads"`
}

type CapacityPlanWorkload struct {
	Timeframe         string `yaml:"timeframe"`
	Instruments       int    `yaml:"instruments"`
	RequestsPerCandle int    `yaml:"requests_per_candle"`
	FreshnessSLAMS    int64  `yaml:"freshness_sla_ms"`
	Source            string `yaml:"source"`
}

type CapacityPlanResult struct {
	Mode     string
	Valid    bool
	Ready    bool
	Forecast capacity.Result
	Err      error
}

var capacityTimeframes = map[string]time.Duration{
	"1m": time.Minute, "5m": 5 * time.Minute, "15m": 15 * time.Minute,
	"30m": 30 * time.Minute, "1h": time.Hour, "1d": 24 * time.Hour,
}

// EvaluateCapacityPlan validates and calculates the plan. off is deliberately
// a no-op. shadow records a forecast but is always ready; enforce is startup
// fail-closed and is never a per-request admission shortcut.
func EvaluateCapacityPlan(cfg Config) CapacityPlanResult {
	p := cfg.CapacityPlan
	result := CapacityPlanResult{Mode: p.Mode, Valid: true, Ready: true}
	if p.Mode == "" || p.Mode == "off" {
		result.Mode = "off"
		return result
	}
	if err := validateCapacityPlan(cfg); err != nil {
		return capacityPlanFailure(p.Mode, err)
	}
	loads := make([]capacity.TimeframeLoad, 0, len(p.Workloads))
	for _, w := range p.Workloads {
		source := capacity.SourceExternal
		if w.Source == "derived" {
			source = capacity.SourceDerived
		}
		loads = append(loads, capacity.TimeframeLoad{
			Period: capacityTimeframes[w.Timeframe], Instruments: w.Instruments,
			RequestsPerCandle: w.RequestsPerCandle,
			FreshnessSLA:      time.Duration(w.FreshnessSLAMS) * time.Millisecond, Source: source,
		})
	}
	forecast, err := capacity.Calculate(capacity.Input{
		Config:    capacity.GatewayConfig{SafeBudgetPerMinute: cfg.Providers.TBank.SafeBudgetPerMinute, TargetUtilization: p.TargetUtilization, RetryFactor: p.RetryFactor},
		Workloads: loads,
	})
	if err != nil {
		return capacityPlanFailure(p.Mode, err)
	}
	result.Forecast = forecast
	result.Valid = true
	result.Ready = p.Mode != "enforce" || (forecast.Feasible && forecast.RequiredGateways == 1)
	if p.Mode == "enforce" && !result.Ready {
		result.Err = fmt.Errorf("capacity plan is infeasible: required_gateways=%d feasible=%t", forecast.RequiredGateways, forecast.Feasible)
	}
	return result
}

func capacityPlanFailure(mode string, err error) CapacityPlanResult {
	return CapacityPlanResult{Mode: mode, Valid: false, Ready: mode != "enforce", Err: err}
}

func validateCapacityPlan(cfg Config) error {
	p := cfg.CapacityPlan
	if p.Mode != "shadow" && p.Mode != "enforce" {
		return fmt.Errorf("capacity_plan.mode must be off, shadow, or enforce")
	}
	if p.CredentialGroup == "" || p.CredentialGroup != cfg.Providers.TBank.CredentialGroup {
		return fmt.Errorf("capacity_plan.credential_group must match the unique tbank credential group")
	}
	if p.GatewayCount != 1 {
		return fmt.Errorf("capacity_plan.gateway_count must be 1 for the shared tbank credential group")
	}
	if p.TargetUtilization <= 0 || p.TargetUtilization > 1 || math.IsNaN(p.TargetUtilization) || math.IsInf(p.TargetUtilization, 0) {
		return fmt.Errorf("capacity_plan.target_utilization must be in (0,1]")
	}
	if p.RetryFactor < 1 || math.IsNaN(p.RetryFactor) || math.IsInf(p.RetryFactor, 0) {
		return fmt.Errorf("capacity_plan.retry_factor must be finite and at least 1")
	}
	seen := map[string]bool{}
	for _, w := range p.Workloads {
		if seen[w.Timeframe] {
			return fmt.Errorf("capacity_plan.workloads contains duplicate timeframe %q", w.Timeframe)
		}
		seen[w.Timeframe] = true
		if _, ok := capacityTimeframes[w.Timeframe]; !ok {
			return fmt.Errorf("capacity_plan.workloads contains unsupported timeframe %q", w.Timeframe)
		}
		if w.Instruments < 0 || w.RequestsPerCandle <= 0 || w.FreshnessSLAMS <= 0 {
			return fmt.Errorf("invalid capacity workload %q", w.Timeframe)
		}
		if w.Source != "external" && w.Source != "derived" {
			return fmt.Errorf("capacity workload %q source must be external or derived", w.Timeframe)
		}
		if p.Mode == "enforce" && w.Source != "external" {
			return fmt.Errorf("derived capacity workloads are not allowed in enforce mode")
		}
	}
	if len(p.Workloads) == 0 {
		return fmt.Errorf("capacity_plan.workloads must not be empty")
	}
	if p.Mode == "enforce" {
		// Warmup is the only currently reviewed static universe. Requiring exact
		// equality prevents a YAML edit from changing which external series are fetched.
		if !cfg.Warmup.Enabled {
			return fmt.Errorf("enforce requires the reviewed warmup universe")
		}
		if len(cfg.Warmup.Timeframes) != len(seen) {
			return fmt.Errorf("enforce workload universe does not match warmup universe")
		}
		for _, tf := range cfg.Warmup.Timeframes {
			if !seen[tf] {
				return fmt.Errorf("enforce workload universe missing warmup timeframe %q", tf)
			}
		}
	}
	return nil
}
