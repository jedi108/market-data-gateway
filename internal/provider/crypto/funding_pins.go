package crypto

// BTCUSDT January 2024: archive and Binance's published CHECKSUM were
// independently downloaded and compared by the orchestrator on 2026-11-23.
// https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip
// The companion URL is the archive URL with .CHECKSUM appended.
func fundingArchivePin(venue, symbol, month string) (string, bool) {
	if venue == "binance" && symbol == "BTCUSDT" && month == "2024-01" {
		return "3e0d30870672aa8f0f937881056e3cfd55913ae5c780cd50b33f2763aa0ba58e", true
	}
	return "", false
}
