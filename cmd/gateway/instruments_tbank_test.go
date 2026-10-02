package main

import (
	"strings"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/model"
	tbankprovider "github.com/jedi108/market-data-gateway/internal/provider/tbank"
)

// reviewedWhitelist pins the expected 40-symbol share universe served by the
// registry. AFR is not part of it: TBank does not list the instrument at all.
var reviewedWhitelist = []string{
	"GAZP", "LKOH", "YDEX", "SBER", "VTB", "MOEX", "IRAO", "NLMK", "GCHE",
	"RASP", "HEAD", "VKCO", "ROSN", "ASTR", "AFLT", "FLOT", "MAGN",
	"POSI", "MTSS", "RUAL", "OZON", "T", "X5", "MGNT", "SMLT", "ALRS",
	"PLZL", "SIBN", "TATN", "NVTK", "GMKN", "SGZH", "UPRO", "SPBE", "AFKS",
	"CHMF", "RNFT", "RAGR", "SFIN", "SVCB",
}

// verifiedGoldFuture pins the reviewed physical GOLD contract admitted by the
// F20 online DoD (2026-10-02 live read-only verification). The uid and
// expiration are the verified provider values; a different contract may only
// replace it through the same reviewed verification.
var verifiedGoldFuture = tbankprovider.Instrument{
	CanonicalSymbol:      "GOLD-12.26",
	ProviderInstrumentID: "91f84d07-14a1-43a6-a6b8-648b62a41994",
	MarketType:           "futures",
	Aliases:              []string{"GOLD", "GDZ6"},
	InstrumentType:       model.InstrumentTypeFuture,
	ProviderIDKind:       model.ProviderIDKindInstrumentUID,
	PriceUnit:            model.PriceUnitPoints,
	VolumeUnit:           model.VolumeUnitLots,
	ExpirationUTCMS:      1797811200000, // 2026-12-21T00:00:00Z
}

func TestTbankInstrumentsBuildValidRegistry(t *testing.T) {
	registry, err := tbankprovider.NewRegistry(tbankInstruments())
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	if len(tbankInstruments()) != 41 {
		t.Fatalf("instruments=%d, want 41 (40 reviewed shares + 1 verified GOLD future; AFR delisted)", len(tbankInstruments()))
	}
	if _, err := registry.Resolve("SBER"); err != nil {
		t.Fatalf("resolve SBER: %v", err)
	}
	if got, err := registry.Resolve("sber/rub"); err != nil || got.ProviderInstrumentID != "BBG004730N88" {
		t.Fatalf("resolve sber/rub = %+v err=%v", got, err)
	}
	// Pre-rename VTB whitelist spelling must resolve to Bank VTB (VTBR).
	if got, err := registry.Resolve("VTB/RUB"); err != nil || got.ProviderInstrumentID != "BBG004730ZJ9" {
		t.Fatalf("resolve VTB/RUB = %+v err=%v", got, err)
	}
}

func TestTbankInstrumentsCoverReviewedWhitelist(t *testing.T) {
	registry, err := tbankprovider.NewRegistry(tbankInstruments())
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	for _, ticker := range reviewedWhitelist {
		if _, err := registry.Resolve(ticker + "/RUB"); err != nil {
			t.Fatalf("whitelist pair %s/RUB does not resolve: %v", ticker, err)
		}
	}
}

// TestTbankInstrumentsProductionFuturesAreVerifiedGold guards the F20
// boundary: the production list carries exactly one futures instrument and it
// is the reviewed physical GOLD contract admitted by the live online
// verification (uid + expiration + units pinned). Synthetic fixtures and any
// assumed/foreign contract stay forbidden: a futures row that does not match
// the reviewed values fails the build.
func TestTbankInstrumentsProductionFuturesAreVerifiedGold(t *testing.T) {
	var futures []tbankprovider.Instrument
	for _, instrument := range tbankInstruments() {
		if instrument.MarketType == "futures" {
			futures = append(futures, instrument)
			continue
		}
		if instrument.InstrumentType != "" && instrument.InstrumentType != "share" {
			t.Fatalf("production instrument %q declares type %q", instrument.CanonicalSymbol, instrument.InstrumentType)
		}
		if instrument.ExpirationUTCMS != 0 {
			t.Fatalf("production instrument %q carries an expiration", instrument.CanonicalSymbol)
		}
		if strings.HasPrefix(instrument.ProviderInstrumentID, "synthetic-") {
			t.Fatalf("synthetic fixture %q must never enter the production list", instrument.ProviderInstrumentID)
		}
	}
	if len(futures) != 1 {
		t.Fatalf("production futures rows=%d, want exactly the 1 verified GOLD contract", len(futures))
	}
	gold := futures[0]
	if gold.ProviderInstrumentID != verifiedGoldFuture.ProviderInstrumentID {
		t.Fatalf("futures uid %q is not the reviewed GOLD contract; a new contract requires the same live verification", gold.ProviderInstrumentID)
	}
	if gold.CanonicalSymbol != verifiedGoldFuture.CanonicalSymbol || gold.ExpirationUTCMS != verifiedGoldFuture.ExpirationUTCMS {
		t.Fatalf("physical contract %q/%d drifted from the reviewed values %q/%d", gold.CanonicalSymbol, gold.ExpirationUTCMS, verifiedGoldFuture.CanonicalSymbol, verifiedGoldFuture.ExpirationUTCMS)
	}
	if gold.InstrumentType != model.InstrumentTypeFuture || gold.ProviderIDKind != model.ProviderIDKindInstrumentUID || gold.PriceUnit != model.PriceUnitPoints || gold.VolumeUnit != model.VolumeUnitLots || gold.Currency != "" {
		t.Fatalf("verified GOLD contract unit shape wrong: %+v", gold)
	}
	// The logical rolling alias targets exactly this active contract, and the
	// physical uid resolves by itself (series identity).
	registry, err := tbankprovider.NewRegistry(tbankInstruments())
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	if got, err := registry.Resolve("GOLD"); err != nil || got.ProviderInstrumentID != verifiedGoldFuture.ProviderInstrumentID {
		t.Fatalf("resolve GOLD = %+v err=%v", got, err)
	}
	if got, err := registry.Resolve("GDZ6"); err != nil || got.ProviderInstrumentID != verifiedGoldFuture.ProviderInstrumentID {
		t.Fatalf("resolve GDZ6 = %+v err=%v", got, err)
	}
	if got, err := registry.Resolve(verifiedGoldFuture.ProviderInstrumentID); err != nil || got.CanonicalSymbol != verifiedGoldFuture.CanonicalSymbol {
		t.Fatalf("physical uid must resolve as identity: %+v err=%v", got, err)
	}
}
