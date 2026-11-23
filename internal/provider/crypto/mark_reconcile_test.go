package crypto

import (
	"errors"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

func TestReconcileMark(t *testing.T) {
	first := model.Candle{OpenTimeUTCMS: 100, CloseTimeUTCMS: 200, Open: "10", High: "12", Low: "9", Close: "11", Volume: "0", IsClosed: true}
	second := model.Candle{OpenTimeUTCMS: 200, CloseTimeUTCMS: 300, Open: "11", High: "13", Low: "10", Close: "12", Volume: "0", IsClosed: true}
	archive := []model.Candle{first, second}
	api := []model.Candle{first, second}
	api[0].Close = "11.000"
	got, err := ReconcileMark(archive, api, 100, 300, true)
	if err != nil || len(got) != 2 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	got[0].Open = "changed"
	if archive[0].Open == "changed" {
		t.Fatal("returned slice aliases source")
	}
	changed := func(c model.Candle, edit func(*model.Candle)) model.Candle { edit(&c); return c }
	tests := []struct {
		name         string
		archive, api []model.Candle
		from, to     int64
		read         bool
	}{
		{"unverified pin or incomplete reads", archive, api, 100, 300, false},
		{"empty", nil, nil, 100, 300, true},
		{"missing api candle", archive, api[:1], 100, 300, true},
		{"missing archive candle", archive[:1], api, 100, 300, true},
		{"different price", archive, []model.Candle{changed(first, func(c *model.Candle) { c.Close = "10" }), second}, 100, 300, true},
		{"different timestamp", archive, []model.Candle{first, changed(second, func(c *model.Candle) { c.OpenTimeUTCMS++ })}, 100, 300, true},
		{"open candle", archive, []model.Candle{first, changed(second, func(c *model.Candle) { c.IsClosed = false })}, 100, 300, true},
		{"duplicate", []model.Candle{first, first}, []model.Candle{first, first}, 100, 300, true},
		{"outside range", archive, api, 100, 299, true},
		{"invalid range", archive, api, 300, 100, true},
		{"trade volume", archive, []model.Candle{changed(first, func(c *model.Candle) { c.Volume = "1" }), second}, 100, 300, true},
		{"fractional syntax", archive, []model.Candle{changed(first, func(c *model.Candle) { c.Open = "10/1" }), second}, 100, 300, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReconcileMark(tc.archive, tc.api, tc.from, tc.to, tc.read)
			var typed *apperror.Error
			if got != nil || !errors.As(err, &typed) || typed.Code != apperror.CodeCoverageUnknown || typed.Retryable {
				t.Fatalf("expected coverage unknown without data, got %+v, %v", got, err)
			}
		})
	}
}
