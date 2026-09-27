package capacity

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestCapacityReviewedFortySeriesPlan(t *testing.T) {
	result, err := Calculate(Input{
		Config: GatewayConfig{SafeBudgetPerMinute: 300, TargetUtilization: 1, RetryFactor: 1.1},
		Workloads: []TimeframeLoad{
			{Period: 15 * time.Minute, Instruments: 40, RequestsPerCandle: 1, FreshnessSLA: 30 * time.Second, Source: SourceExternal},
			{Period: time.Hour, Instruments: 40, RequestsPerCandle: 1, FreshnessSLA: 30 * time.Second, Source: SourceExternal},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.HardRPS != 5 || result.EffectiveRPS != 5 || result.RequiredRequests != 80 || result.RequiredGateways != 1 || !result.Feasible {
		t.Fatalf("unexpected reviewed plan result: %+v", result)
	}
	if len(result.Boundaries) != 5 || result.BurstRPS != 8.0/3.0 {
		t.Fatalf("unexpected reviewed boundary result: %+v", result)
	}
}

func TestCapacityProperties(t *testing.T) {
	for _, instruments := range []int{0, 1, 7, 40} {
		for _, requests := range []int{1, 2, 5} {
			for _, utilization := range []float64{0.5, 1} {
				in := Input{Config: GatewayConfig{SafeBudgetPerMinute: 300, TargetUtilization: utilization, RetryFactor: 1}, Workloads: []TimeframeLoad{{Period: 15 * time.Minute, Instruments: instruments, RequestsPerCandle: requests, FreshnessSLA: time.Minute, Source: SourceExternal}}}
				result, err := Calculate(in)
				if err != nil {
					t.Fatal(err)
				}
				if result.HardRPS < 0 || result.EffectiveRPS < 0 || result.AverageRPS < 0 || result.BurstRPS < 0 || result.RequiredRequests < 0 || result.RequiredGateways < 0 || result.MaxDrainTime < 0 {
					t.Fatalf("negative result for %+v: %+v", in, result)
				}
				if instruments > 0 && requests > 0 && result.RequiredRequests != instruments*requests {
					t.Fatalf("instrument/request accounting mismatch: %+v", result)
				}
			}
		}
	}
}

func TestCapacityCheckMonotonicWithInstrumentsAndSLA(t *testing.T) {
	base := Input{Config: GatewayConfig{SafeBudgetPerMinute: 60, TargetUtilization: 1, RetryFactor: 1}, Workloads: []TimeframeLoad{{Period: time.Minute, Instruments: 1, RequestsPerCandle: 1, FreshnessSLA: time.Second, Source: SourceExternal}}}
	if err := Check(base); err != nil {
		t.Fatal(err)
	}
	for _, extra := range []int{1, 2, 10} {
		larger := base
		larger.Workloads = []TimeframeLoad{{Period: time.Minute, Instruments: extra, RequestsPerCandle: 1, FreshnessSLA: time.Second, Source: SourceExternal}}
		if err := Check(larger); err != nil {
			t.Fatalf("larger valid instrument set rejected: %v", err)
		}
	}
	invalid := base
	invalid.Config.TargetUtilization = 0
	if err := Check(invalid); err == nil {
		t.Fatal("invalid base unexpectedly accepted")
	}
	invalid.Workloads[0].Instruments = 10
	if err := Check(invalid); err == nil {
		t.Fatal("larger invalid workload unexpectedly repaired invalid config")
	}
	short := base
	short.Workloads[0].FreshnessSLA = 500 * time.Millisecond
	long := base
	long.Workloads[0].FreshnessSLA = 2 * time.Second
	shortResult, _ := Calculate(short)
	longResult, _ := Calculate(long)
	if shortResult.BurstRPS < longResult.BurstRPS {
		t.Fatalf("shorter SLA reduced burst demand: short=%+v long=%+v", shortResult, longResult)
	}
}

func FuzzCapacityInputProperties(f *testing.F) {
	for _, seed := range []struct{ instruments, requests, sla int }{{0, 1, 1}, {40, 1, 30}, {7, 3, 2}} {
		f.Add(seed.instruments, seed.requests, seed.sla)
	}
	f.Fuzz(func(t *testing.T, instruments, requests, sla int) {
		if instruments < 0 || instruments > 1000 || requests <= 0 || requests > 100 || sla <= 0 || sla > 3600 {
			t.Skip()
		}
		in := Input{Config: GatewayConfig{SafeBudgetPerMinute: 300, TargetUtilization: 1, RetryFactor: 1.1}, Workloads: []TimeframeLoad{{Period: 15 * time.Minute, Instruments: instruments, RequestsPerCandle: requests, FreshnessSLA: time.Duration(sla) * time.Second, Source: SourceExternal}}}
		result, err := Calculate(in)
		if err != nil {
			t.Fatalf("bounded input failed: %v", err)
		}
		if math.Signbit(result.AverageRPS) || math.Signbit(result.BurstRPS) || result.RequiredRequests < 0 || result.RequiredGateways < 0 {
			t.Fatalf("non-negative property failed: %+v", result)
		}
		reversed := in
		reversed.Workloads = append([]TimeframeLoad(nil), in.Workloads...)
		if result2, err := Calculate(reversed); err != nil || !reflect.DeepEqual(result, result2) {
			t.Fatalf("order invariance failed: %+v / %v", result2, err)
		}
		if err := Check(in); err != nil {
			t.Fatalf("Check unexpectedly rejected Calculate-valid input: %v", err)
		}
	})
}
