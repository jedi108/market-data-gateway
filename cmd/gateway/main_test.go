package main

import (
	"context"
	"errors"
	"github.com/jedi108/market-data-gateway/internal/apperror"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jedi108/market-data-gateway/internal/config"
	"github.com/jedi108/market-data-gateway/internal/model"
)

func TestDiagnosticFundingResolverLimitsContract(t *testing.T) {
	key, err := diagnosticFundingResolver("binance", "BTC/USDT:USDT", "8h")
	if err != nil || key.ProviderInstrumentID != "BTCUSDT" || key.MarketType != "futures" || key.Completeness != "reconciled" || !key.DiagnosticOnly {
		t.Fatalf("%+v %v", key, err)
	}
	for _, input := range [][3]string{{"binance", "BTC/USDT", "8h"}, {"bybit", "BTC/USDT:USDT", "8h"}, {"binance", "ETH/USDT", "8h"}, {"binance", "BTC/USDT", "1h"}} {
		if _, err := diagnosticFundingResolver(input[0], input[1], input[2]); err == nil {
			t.Fatalf("unexpected resolution %v", input)
		}
	}
}

func TestFakeCryptoModeKeepsBinanceSpotTrade(t *testing.T) {
	key, err := resolver(nil)("binance", "BTC/USDT", "1h")
	if err != nil || key.MarketType != "spot" || key.CandleType != "trade" {
		t.Fatalf("spot key %+v: %v", key, err)
	}
	p, err := buildProvider("fake", "fake", config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli() / 3600000 * 3600000
	candles, _, err := p.FetchCandles(context.Background(), model.CandleRequest{Series: key, FromUTCMS: now - 3600000, ToUTCMS: now, Limit: 1})
	if err != nil || len(candles) != 1 {
		t.Fatalf("spot trade candles %+v: %v", candles, err)
	}
}

// Diagnostic mode must not return synthetic spot candles as real market data.
func TestDiagnosticModeBinanceSpotFailsClosed(t *testing.T) {
	key, err := resolver(nil)("binance", "BTC/USDT", "1h")
	if err != nil {
		t.Fatal(err)
	}
	p, err := buildProvider("fake", "diagnostic", config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = p.FetchCandles(context.Background(), model.CandleRequest{Series: key, FromUTCMS: 0, ToUTCMS: 3600000, Limit: 1})
	var typed *apperror.Error
	if !errors.As(err, &typed) || typed.Code != apperror.CodeCoverageUnknown {
		t.Fatalf("expected coverage unknown, got %v", err)
	}
}
