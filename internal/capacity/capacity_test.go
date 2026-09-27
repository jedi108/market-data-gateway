package capacity

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestCalculateAverageAndDerivedWorkload(t *testing.T) {
	result, err := Calculate(Input{Config: GatewayConfig{SafeBudgetPerMinute: 600, TargetUtilization: .8, RetryFactor: 1.1}, Workloads: []TimeframeLoad{
		{Period: time.Minute, Instruments: 10, RequestsPerCandle: 1, FreshnessSLA: 10 * time.Second, Source: SourceExternal},
		{Period: 5 * time.Minute, Instruments: 100, RequestsPerCandle: 1, FreshnessSLA: time.Minute, Source: SourceDerived},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result.HardRPS != 10 || result.EffectiveRPS != 8 || math.Abs(result.AverageRPS-.1833333333) > 1e-9 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.RequiredRequests != 10 || result.RequiredGateways != 1 || !result.Feasible {
		t.Fatalf("unexpected capacity: %+v", result)
	}
}

func TestCalculateOverlappingBoundariesAndOrder(t *testing.T) {
	loads := []TimeframeLoad{
		{Period: time.Minute, Instruments: 4, RequestsPerCandle: 1, FreshnessSLA: 10 * time.Second, Source: SourceExternal},
		{Period: 2 * time.Minute, Instruments: 6, RequestsPerCandle: 1, FreshnessSLA: 10 * time.Second, Source: SourceExternal},
	}
	cfg := GatewayConfig{SafeBudgetPerMinute: 60, TargetUtilization: 1, RetryFactor: 2}
	a, err := Calculate(Input{Config: cfg, Workloads: loads})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Calculate(Input{Config: cfg, Workloads: []TimeframeLoad{loads[1], loads[0]}})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Boundaries) != 3 || a.Boundaries[0].Requests != 10 || a.Boundaries[1].Requests != 4 {
		t.Fatalf("unexpected boundaries: %+v", a.Boundaries)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("order changed result: %+v vs %+v", a, b)
	}
	if a.BurstRPS != 1 || a.RequiredGateways != 1 || !a.Feasible {
		t.Fatalf("unexpected feasibility: %+v", a)
	}
}

func TestValidationAndOverflowSentinels(t *testing.T) {
	cases := []Input{
		{Config: GatewayConfig{SafeBudgetPerMinute: 60, TargetUtilization: 1, RetryFactor: 1}, Workloads: []TimeframeLoad{{Period: time.Minute, Instruments: -1, RequestsPerCandle: 1, FreshnessSLA: time.Second, Source: SourceExternal}}},
		{Config: GatewayConfig{SafeBudgetPerMinute: 60, TargetUtilization: 1, RetryFactor: 1}, Workloads: []TimeframeLoad{{Period: time.Minute, Instruments: 1, RequestsPerCandle: 1, FreshnessSLA: 0, Source: SourceExternal}}},
	}
	for _, input := range cases {
		if _, err := Calculate(input); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("error=%v", err)
		}
	}
	_, err := Calculate(Input{Config: GatewayConfig{SafeBudgetPerMinute: 60, TargetUtilization: 1, RetryFactor: 1}, Workloads: []TimeframeLoad{{Period: time.Nanosecond, Instruments: math.MaxInt, RequestsPerCandle: 2, FreshnessSLA: time.Second, Source: SourceExternal}}})
	if !errors.Is(err, ErrOverflow) {
		t.Fatalf("overflow error=%v", err)
	}
}

func TestEmptyWorkload(t *testing.T) {
	result, err := Calculate(Input{Config: GatewayConfig{SafeBudgetPerMinute: 60, TargetUtilization: 1, RetryFactor: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Feasible || result.RequiredGateways != 0 || len(result.Boundaries) != 0 {
		t.Fatalf("unexpected empty result: %+v", result)
	}
}

func TestPathologicalScheduleReturnsOverflow(t *testing.T) {
	_, err := Calculate(Input{Config: GatewayConfig{SafeBudgetPerMinute: 600, TargetUtilization: 1, RetryFactor: 1}, Workloads: []TimeframeLoad{{Period: time.Microsecond, Instruments: 1, RequestsPerCandle: 1, FreshnessSLA: time.Second, Source: SourceExternal}}})
	if !errors.Is(err, ErrOverflow) {
		t.Fatalf("error=%v", err)
	}
}
