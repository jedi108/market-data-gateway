package provider

import (
	"context"
	"github.com/jedi108/market-data-gateway/internal/model"
)

type Provider interface {
	FetchCandles(context.Context, model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error)
}

// FundingProvider returns independently reconciled historical funding events
// for the entire requested diagnostic window, or an error if it is unknown.
type FundingProvider interface {
	FetchFunding(context.Context, model.CandleRequest) ([]model.FundingEvent, error)
}
