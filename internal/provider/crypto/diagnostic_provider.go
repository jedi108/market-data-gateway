package crypto

import (
	"context"
	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
	"net/http"
)

// DiagnosticProvider never treats ordinary klines or a single mark source as diagnostic proof.
type DiagnosticProvider struct {
	funding *BinanceFundingSource
	mark    *ReconciledMarkSource
}

func NewDiagnosticProvider(client *http.Client) *DiagnosticProvider {
	return &DiagnosticProvider{funding: NewBinanceFundingSource(client), mark: NewReconciledMarkSource(client)}
}
func (p *DiagnosticProvider) FetchCandles(ctx context.Context, request model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	key := request.Series
	if key.Validate() != nil || key.CandleType != "mark" || key.Completeness != "reconciled" || request.IncludeIncomplete || p == nil || p.mark == nil {
		return nil, model.ProviderMetadata{}, apperror.New(apperror.CodeCoverageUnknown, "diagnostic candle source unavailable", 0)
	}
	candles, err := p.mark.Fetch(ctx, key, request.FromUTCMS, request.ToUTCMS, request.Limit)
	return candles, model.ProviderMetadata{}, err
}
func (p *DiagnosticProvider) FetchFunding(ctx context.Context, request model.CandleRequest) ([]model.FundingEvent, error) {
	key := request.Series
	if key.Validate() != nil || key.CandleType != "funding" || key.Completeness != "reconciled" || request.IncludeIncomplete {
		return nil, apperror.New(apperror.CodeCoverageUnknown, "funding diagnostic unavailable", 0)
	}
	return p.funding.Fetch(ctx, key.ProviderInstrumentID, request.FromUTCMS, request.ToUTCMS)
}
