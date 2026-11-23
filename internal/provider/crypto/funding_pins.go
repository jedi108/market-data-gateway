package crypto

// fundingArchivePin is a manually pinned Binance USD-M monthly archive SHA-256.
// A newly downloaded Binance checksum alone is never an acceptable pin.
// BTCUSDT 2024-01 was pinned from data.binance.vision/data/futures/um/monthly/
// fundingRate/BTCUSDT/ after independently hashing the archive and matching its
// published .CHECKSUM; a match does not establish coverage completeness.
func fundingArchivePin(venue, symbol, month string) (string, bool) {
	if venue == "binance" && symbol == "BTCUSDT" && month == "2024-01" {
		return "3e0d30870672aa8f0f937881056e3cfd55913ae5c780cd50b33f2763aa0ba58e", true
	}
	return "", false
}
