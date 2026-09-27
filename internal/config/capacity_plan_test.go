package config

import "testing"

func capacityTestConfig() Config {
	return Config{
		Providers:    Providers{TBank: TBank{CredentialGroup: "shared", SafeBudgetPerMinute: 60}},
		Warmup:       Warmup{Enabled: true, Timeframes: []string{"15m"}},
		CapacityPlan: CapacityPlan{Mode: "shadow", CredentialGroup: "shared", GatewayCount: 1, TargetUtilization: 1, RetryFactor: 1.1, Workloads: []CapacityPlanWorkload{{Timeframe: "15m", Instruments: 1, RequestsPerCandle: 1, FreshnessSLAMS: 30000, Source: "external"}}},
	}
}

func TestCapacityPlanModes(t *testing.T) {
	cfg := capacityTestConfig()
	if got := EvaluateCapacityPlan(cfg); got.Err != nil || !got.Ready || got.Forecast.HardRPS != 1 {
		t.Fatalf("shadow: %+v", got)
	}
	cfg.CapacityPlan.Mode = "enforce"
	got := EvaluateCapacityPlan(cfg)
	if got.Err != nil || !got.Ready || got.Forecast.RequiredGateways != 1 {
		t.Fatalf("enforce: %+v", got)
	}
	cfg.CapacityPlan.Workloads[0].Source = "derived"
	got = EvaluateCapacityPlan(cfg)
	if got.Err == nil || got.Ready {
		t.Fatalf("derived enforce must fail closed: %+v", got)
	}
}

func TestCapacityPlanDoesNotMultiplySharedBudget(t *testing.T) {
	cfg := capacityTestConfig()
	cfg.CapacityPlan.GatewayCount = 2
	got := EvaluateCapacityPlan(cfg)
	if got.Err == nil || !got.Ready {
		t.Fatalf("shared group must reject gateway multiplication: %+v", got)
	}
}

func TestCapacityPlanReviewedSharedGroupWorkload(t *testing.T) {
	cfg := capacityTestConfig()
	cfg.Providers.TBank.SafeBudgetPerMinute = 300
	cfg.Warmup.Timeframes = []string{"15m", "1h"}
	cfg.CapacityPlan.Workloads = []CapacityPlanWorkload{
		{Timeframe: "15m", Instruments: 40, RequestsPerCandle: 1, FreshnessSLAMS: 30000, Source: "external"},
		{Timeframe: "1h", Instruments: 40, RequestsPerCandle: 1, FreshnessSLAMS: 30000, Source: "external"},
	}
	cfg.CapacityPlan.Mode = "enforce"
	got := EvaluateCapacityPlan(cfg)
	if got.Err != nil || !got.Valid || !got.Ready {
		t.Fatalf("reviewed shared-group plan must be ready: %+v", got)
	}
	if got.Forecast.RequiredGateways != 1 || got.Forecast.RequiredRequests != 80 {
		t.Fatalf("shared budget was multiplied or accounting changed: %+v", got.Forecast)
	}
}

func TestCapacityPlanShadowIsNeutralForInvalidForecast(t *testing.T) {
	cfg := capacityTestConfig()
	cfg.CapacityPlan.Mode = "shadow"
	cfg.CapacityPlan.Workloads[0].FreshnessSLAMS = 1
	cfg.CapacityPlan.Workloads[0].Instruments = 1000
	got := EvaluateCapacityPlan(cfg)
	if !got.Valid || !got.Ready {
		t.Fatalf("shadow must not fail readiness: %+v", got)
	}
}
