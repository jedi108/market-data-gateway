package provider

import (
	"context"
	"github.com/jedi108/market-data-gateway/internal/model"
)

type Provider interface {
	FetchCandles(context.Context, model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error)
}
