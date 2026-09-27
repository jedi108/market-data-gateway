package tbank

import (
	"context"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
	investapi "github.com/jedi108/market-data-gateway/internal/provider/tbank/gen/investapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeMarketData struct {
	response *investapi.GetCandlesResponse
	err      error
	request  *investapi.GetCandlesRequest
}

func (f *fakeMarketData) GetCandles(_ context.Context, req *investapi.GetCandlesRequest, _ ...grpc.CallOption) (*investapi.GetCandlesResponse, error) {
	f.request = req
	return f.response, f.err
}

func TestProviderMapsGatewayModelsOnly(t *testing.T) {
	client := &fakeMarketData{response: &investapi.GetCandlesResponse{Candles: []*investapi.HistoricCandle{{Open: &investapi.Quotation{Units: 10}, High: &investapi.Quotation{Units: 12}, Low: &investapi.Quotation{Units: 9}, Close: &investapi.Quotation{Units: 11}, Volume: 7, Time: timestamppb.New(time.Unix(1_700_000_000, 0)), IsComplete: true}}}}
	provider := New(client)
	candles, _, err := provider.FetchCandles(context.Background(), model.CandleRequest{Series: model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "BBG004730N88", Timeframe: "1h", CandleType: "trade"}, FromUTCMS: 1, ToUTCMS: 2, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(candles) != 1 || candles[0].Open != "10" || candles[0].Volume != "7" || client.request.GetInstrumentId() != "BBG004730N88" {
		t.Fatalf("unexpected result: %#v, request=%#v", candles, client.request)
	}
}

func TestProviderMapsRateLimitError(t *testing.T) {
	provider := New(&fakeMarketData{err: status.Error(codes.ResourceExhausted, "limited")})
	_, _, err := provider.FetchCandles(context.Background(), model.CandleRequest{Series: model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "id", Timeframe: "1h", CandleType: "trade"}, FromUTCMS: 1, ToUTCMS: 2})
	providerErr, ok := err.(*apperror.Error)
	if !ok || providerErr.Code != apperror.CodeProviderRateLimited {
		t.Fatalf("unexpected error: %T %[1]v", err)
	}
}
