package crypto

import "testing"

func TestFundingArchivePin(t *testing.T) {
	pin, ok := fundingArchivePin("binance", "BTCUSDT", "2024-01")
	if !ok || pin != "3e0d30870672aa8f0f937881056e3cfd55913ae5c780cd50b33f2763aa0ba58e" {
		t.Fatalf("unexpected pin: %q, %v", pin, ok)
	}
	for _, key := range [][3]string{{"bybit", "BTCUSDT", "2024-01"}, {"binance", "ETHUSDT", "2024-01"}, {"binance", "BTCUSDT", "2024-02"}} {
		if _, ok := fundingArchivePin(key[0], key[1], key[2]); ok {
			t.Fatalf("unpinned archive accepted: %v", key)
		}
	}
}
