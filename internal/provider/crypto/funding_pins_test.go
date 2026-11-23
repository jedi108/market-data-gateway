package crypto

import "testing"

func TestFundingArchivePinFailsClosedWithoutEvidence(t *testing.T) {
	for _, key := range [][3]string{{"binance", "BTCUSDT", "2024-01"}, {"bybit", "BTCUSDT", "2024-01"}, {"binance", "ETHUSDT", "2024-01"}} {
		if _, ok := fundingArchivePin(key[0], key[1], key[2]); ok {
			t.Fatalf("unverified archive accepted: %v", key)
		}
	}
}
