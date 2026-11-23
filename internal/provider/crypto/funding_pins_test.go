package crypto

import "testing"

func TestFundingArchivePinFailsClosedWithoutEvidence(t *testing.T) {
	for _, key := range [][3]string{{"bybit", "BTCUSDT", "2024-01"}, {"binance", "ETHUSDT", "2024-01"}} {
		if _, ok := fundingArchivePin(key[0], key[1], key[2]); ok {
			t.Fatalf("unverified archive accepted: %v", key)
		}
	}
}

func TestFundingArchivePinBTCUSDTJanuary2024(t *testing.T) {
	got, ok := fundingArchivePin("binance", "BTCUSDT", "2024-01")
	if !ok || got != "3e0d30870672aa8f0f937881056e3cfd55913ae5c780cd50b33f2763aa0ba58e" {
		t.Fatalf("pin=%q ok=%v", got, ok)
	}
}
