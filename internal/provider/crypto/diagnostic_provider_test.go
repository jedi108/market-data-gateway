package crypto

import (
	"context"
	"errors"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

func TestDiagnosticProviderFetchFundingMissingSource(t *testing.T) {
	request := model.CandleRequest{Series: model.SeriesKey{Venue: "binance", MarketType: "futures", ProviderInstrumentID: "BTCUSDT", Timeframe: "8h", CandleType: "funding", Completeness: "reconciled", DiagnosticOnly: true}}
	for _, provider := range []*DiagnosticProvider{nil, {}} {
		events, err := provider.FetchFunding(context.Background(), request)
		var typed *apperror.Error
		if events != nil || !errors.As(err, &typed) || typed.Code != apperror.CodeCoverageUnknown || typed.Retryable {
			t.Fatalf("events=%v err=%v", events, err)
		}
	}
}
