package crypto

import (
	"errors"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

func TestReconcileFunding(t *testing.T) {
	archive := []model.FundingEvent{{Ts: 100, Rate: "0.0100", IntervalH: 8, AvailableAt: 100}, {Ts: 200, Rate: "-0.02", IntervalH: 4, AvailableAt: 210}}
	api := []FundingAPIRate{{Ts: 100, Rate: "0.01"}, {Ts: 200, Rate: "-0.020"}}
	got, err := ReconcileFunding(archive, api, 100, 300, true)
	if err != nil || len(got) != 2 || got[0].IntervalH != 8 || got[1].IntervalH != 4 || got[1].AvailableAt != 210 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	got[0].Rate = "changed"
	if archive[0].Rate == "changed" {
		t.Fatal("returned slice aliases source")
	}
	tests := []struct {
		name     string
		archive  []model.FundingEvent
		api      []FundingAPIRate
		from, to int64
		read     bool
	}{
		{"not fully read", archive, api, 100, 300, false},
		{"empty", nil, nil, 100, 300, true},
		{"missing api event", archive, api[:1], 100, 300, true},
		{"missing archive event", archive[:1], api, 100, 300, true},
		{"different timestamp", archive, []FundingAPIRate{{100, "0.01"}, {201, "-0.02"}}, 100, 300, true},
		{"different rate", archive, []FundingAPIRate{{100, "0.02"}, {200, "-0.02"}}, 100, 300, true},
		{"invalid rate", archive, []FundingAPIRate{{100, "1/100"}, {200, "-0.02"}}, 100, 300, true},
		{"out of range", archive, api, 100, 200, true},
		{"invalid window", archive, api, 300, 100, true},
		{"duplicates", []model.FundingEvent{archive[0], archive[0]}, []FundingAPIRate{api[0], api[0]}, 100, 300, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReconcileFunding(tc.archive, tc.api, tc.from, tc.to, tc.read)
			var typed *apperror.Error
			if !errors.As(err, &typed) || typed.Code != apperror.CodeCoverageUnknown || typed.Retryable {
				t.Fatalf("expected coverage unknown, got %v", err)
			}
		})
	}
}
