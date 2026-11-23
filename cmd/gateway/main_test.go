package main

import "testing"

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
