package tbank

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
	investapi "github.com/jedi108/market-data-gateway/internal/provider/tbank/gen/investapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MarketDataClient interface {
	GetCandles(context.Context, *investapi.GetCandlesRequest, ...grpc.CallOption) (*investapi.GetCandlesResponse, error)
}
type Provider struct{ client MarketDataClient }

// F20 futures note: the generated stubs require no per-asset-class widening.
// GetCandlesRequest.instrument_id accepts either a share FIGI or a physical
// futures contract instrument_uid, so physical futures candles use the same
// MarketDataService.GetCandles RPC and the same FetchCandles path below — the
// identity difference lives entirely in the registry/series key (instrument
// UID per expiry). No order/trading RPC is referenced anywhere in the gateway.
//
// TODO(F20 online DoD): verify against live TBank that (a) the physical GOLD
// contract UID returns candles through instrument_id exactly as a FIGI does,
// and (b) futures quotations are delivered in points with lot-denominated
// volume as assumed by model.InstrumentMetadata defaults. Until then no real
// futures instrument is added to the production registry.

func New(client MarketDataClient) *Provider { return &Provider{client: client} }

// FetchCandlesWithHeaders performs the same call as FetchCandles and also
// returns response header metadata so callers can extract x-ratelimit-*.
func (p *Provider) FetchCandlesWithHeaders(ctx context.Context, request model.CandleRequest) ([]model.Candle, model.ProviderMetadata, map[string][]string, error) {
	headerMD := metadata.MD{}
	candles, meta, err := p.fetchCandles(ctx, request, grpc.Header(&headerMD))
	if err != nil {
		return candles, meta, nil, err
	}
	if remaining := RatelimitRemainingFromHeader(headerMD); remaining >= 0 {
		meta.RateLimitRemaining = remaining
	}
	return candles, meta, headerMD, nil
}

func (p *Provider) FetchCandles(ctx context.Context, request model.CandleRequest) ([]model.Candle, model.ProviderMetadata, error) {
	candles, meta, err := p.fetchCandles(ctx, request)
	return candles, meta, err
}

func (p *Provider) fetchCandles(ctx context.Context, request model.CandleRequest, options ...grpc.CallOption) ([]model.Candle, model.ProviderMetadata, error) {
	if err := request.Series.Validate(); err != nil {
		return nil, model.ProviderMetadata{}, apperror.New(apperror.CodeInvalidRequest, err.Error(), 0)
	}
	interval, duration, err := candleInterval(request.Series.Timeframe)
	if err != nil {
		return nil, model.ProviderMetadata{}, err
	}
	protoRequest := &investapi.GetCandlesRequest{InstrumentId: &request.Series.ProviderInstrumentID, Interval: interval}
	if request.FromUTCMS > 0 {
		protoRequest.From = timestamppb.New(time.UnixMilli(request.FromUTCMS))
	}
	if request.ToUTCMS > 0 {
		protoRequest.To = timestamppb.New(time.UnixMilli(request.ToUTCMS))
	}
	if request.Limit > 0 {
		limit := int32(request.Limit)
		protoRequest.Limit = &limit
	}
	response, callErr := p.client.GetCandles(ctx, protoRequest, options...)
	if callErr != nil {
		return nil, model.ProviderMetadata{}, mapError(callErr)
	}
	candles := make([]model.Candle, 0, len(response.GetCandles()))
	for _, source := range response.GetCandles() {
		candle, err := mapCandle(source, duration)
		if err != nil {
			return nil, model.ProviderMetadata{}, err
		}
		if !request.IncludeIncomplete && !candle.IsClosed {
			continue
		}
		candles = append(candles, candle)
	}
	return candles, model.ProviderMetadata{Status: "ok"}, nil
}

func candleInterval(timeframe string) (investapi.CandleInterval, time.Duration, error) {
	switch timeframe {
	case "1m":
		return investapi.CandleInterval_CANDLE_INTERVAL_1_MIN, time.Minute, nil
	case "5m":
		return investapi.CandleInterval_CANDLE_INTERVAL_5_MIN, 5 * time.Minute, nil
	case "15m":
		return investapi.CandleInterval_CANDLE_INTERVAL_15_MIN, 15 * time.Minute, nil
	case "1h":
		return investapi.CandleInterval_CANDLE_INTERVAL_HOUR, time.Hour, nil
	case "1d":
		return investapi.CandleInterval_CANDLE_INTERVAL_DAY, 24 * time.Hour, nil
	default:
		return investapi.CandleInterval_CANDLE_INTERVAL_UNSPECIFIED, 0, apperror.New(apperror.CodeInvalidRequest, "unsupported TBank timeframe", 0)
	}
}
func mapCandle(source *investapi.HistoricCandle, duration time.Duration) (model.Candle, error) {
	if source == nil || source.GetTime() == nil {
		return model.Candle{}, apperror.New(apperror.CodeProviderUnavailable, "invalid candle response", 0)
	}
	open, err := quotation(source.GetOpen())
	if err != nil {
		return model.Candle{}, err
	}
	high, err := quotation(source.GetHigh())
	if err != nil {
		return model.Candle{}, err
	}
	low, err := quotation(source.GetLow())
	if err != nil {
		return model.Candle{}, err
	}
	closeValue, err := quotation(source.GetClose())
	if err != nil {
		return model.Candle{}, err
	}
	candle := model.Candle{OpenTimeUTCMS: source.GetTime().AsTime().UnixMilli(), CloseTimeUTCMS: source.GetTime().AsTime().Add(duration).UnixMilli(), Open: open, High: high, Low: low, Close: closeValue, Volume: strconv.FormatInt(source.GetVolume(), 10), IsClosed: source.GetIsComplete()}
	if err := candle.Validate(); err != nil {
		return model.Candle{}, apperror.New(apperror.CodeProviderUnavailable, fmt.Sprintf("invalid TBank candle: %v", err), 0)
	}
	return candle, nil
}
func quotation(value *investapi.Quotation) (string, error) {
	if value == nil {
		return "", apperror.New(apperror.CodeProviderUnavailable, "missing quotation", 0)
	}
	rational := new(big.Rat).SetInt64(value.GetUnits())
	rational.Add(rational, new(big.Rat).SetFrac(big.NewInt(int64(value.GetNano())), big.NewInt(1_000_000_000)))
	decimal := strings.TrimRight(strings.TrimRight(rational.FloatString(9), "0"), ".")
	if decimal == "-0" {
		decimal = "0"
	}
	return decimal, nil
}
func mapError(err error) error {
	switch status.Code(err) {
	case codes.ResourceExhausted:
		return apperror.New(apperror.CodeProviderRateLimited, "TBank provider rate limit", 0)
	case codes.Unauthenticated, codes.PermissionDenied:
		return apperror.New(apperror.CodeProviderUnauthenticated, "TBank provider authentication failed", 0)
	case codes.InvalidArgument, codes.NotFound:
		return apperror.New(apperror.CodeInvalidRequest, "TBank provider rejected request", 0)
	default:
		return apperror.New(apperror.CodeProviderUnavailable, "TBank provider unavailable", 0)
	}
}
