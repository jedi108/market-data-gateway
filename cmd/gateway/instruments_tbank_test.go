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

// verifiedFutures pin the reviewed physical contracts admitted by the live
// read-only verifications (GOLD: F20 online DoD 2026-10-02; BR/SI/MIX/NG:
// issue #8 verification 2026-10-06). The uids and expirations are the verified
// provider values; a different contract may only replace one through the same
// reviewed verification.
var verifiedFutures = map[string]tbankprovider.Instrument{
	"GOLD-12.26": {
		CanonicalSymbol:      "GOLD-12.26",
		ProviderInstrumentID: "91f84d07-14a1-43a6-a6b8-648b62a41994",
		MarketType:           "futures",
		Aliases:              []string{"GOLD", "GDZ6"},
		InstrumentType:       model.InstrumentTypeFuture,
		ProviderIDKind:       model.ProviderIDKindInstrumentUID,
		PriceUnit:            model.PriceUnitPoints,
		VolumeUnit:           model.VolumeUnitLots,
		ExpirationUTCMS:      1797811200000, // 2026-12-21T00:00:00Z
	},
	"BR-12.26": {
		CanonicalSymbol:      "BR-12.26",
		ProviderInstrumentID: "5f72f0f4-c326-46e6-a2d3-1632c74c76df",
		MarketType:           "futures",
		Aliases:              []string{"BR", "BRZ6"},
		InstrumentType:       model.InstrumentTypeFuture,
		ProviderIDKind:       model.ProviderIDKindInstrumentUID,
		PriceUnit:            model.PriceUnitPoints,
		VolumeUnit:           model.VolumeUnitLots,
		ExpirationUTCMS:      1796169600000, // 2026-12-02T00:00:00Z
	},
	"SI-12.26": {
		CanonicalSymbol:      "SI-12.26",
		ProviderInstrumentID: "2dd5eb6b-7a52-4186-a0ec-9f7f6ed6fbd7",
		MarketType:           "futures",
		Aliases:              []string{"SI", "SIZ6"},
		InstrumentType:       model.InstrumentTypeFuture,
		ProviderIDKind:       model.ProviderIDKindInstrumentUID,
		PriceUnit:            model.PriceUnitPoints,
		VolumeUnit:           model.VolumeUnitLots,
		ExpirationUTCMS:      1797552000000, // 2026-12-18T00:00:00Z
	},
	"MIX-12.26": {
		CanonicalSymbol:      "MIX-12.26",
		ProviderInstrumentID: "3d96c0f8-9e6e-4c0a-bac8-44a5a1db8050",
		MarketType:           "futures",
		Aliases:              []string{"MIX", "MXZ6"},
		InstrumentType:       model.InstrumentTypeFuture,
		ProviderIDKind:       model.ProviderIDKindInstrumentUID,
		PriceUnit:            model.PriceUnitPoints,
		VolumeUnit:           model.VolumeUnitLots,
		ExpirationUTCMS:      1797552000000, // 2026-12-18T00:00:00Z
	},
	"NG-11.26": {
		CanonicalSymbol:      "NG-11.26",
		ProviderInstrumentID: "89efcc3f-ef59-4e26-b2f0-0f659f94ac93",
		MarketType:           "futures",
		Aliases:              []string{"NG", "NGX6"},
		InstrumentType:       model.InstrumentTypeFuture,
		ProviderIDKind:       model.ProviderIDKindInstrumentUID,
		PriceUnit:            model.PriceUnitPoints,
		VolumeUnit:           model.VolumeUnitLots,
		ExpirationUTCMS:      1795651200000, // 2026-11-26T00:00:00Z
	},
}

// verifiedGoldFuture keeps the GOLD pin addressable for tests that target the
// original F20 boundary specifically.
var verifiedGoldFuture = verifiedFutures["GOLD-12.26"]

func TestTbankInstrumentsBuildValidRegistry(t *testing.T) {
	registry, err := tbankprovider.NewRegistry(tbankInstruments())
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	if len(tbankInstruments()) != 45 {
		t.Fatalf("instruments=%d, want 45 (40 reviewed shares + 5 verified futures; AFR delisted)", len(tbankInstruments()))
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

// TestTbankInstrumentsProductionFuturesAreVerified guards the F20 boundary
// (extended by issue #8): the production list carries exactly the reviewed
// physical futures contracts (GOLD + BR + SI + MIX + NG), each matching the
// live-verified uid/expiration/units. Synthetic fixtures and any assumed or
// foreign contract stay forbidden: a futures row that does not match the
// reviewed values fails the build.
func TestTbankInstrumentsProductionFuturesAreVerified(t *testing.T) {
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
	if len(futures) != len(verifiedFutures) {
		t.Fatalf("production futures rows=%d, want exactly the %d verified contracts", len(futures), len(verifiedFutures))
	}
	for _, future := range futures {
		want, reviewed := verifiedFutures[future.CanonicalSymbol]
		if !reviewed {
			t.Fatalf("futures contract %q is not a reviewed physical contract; a new contract requires the same live verification", future.CanonicalSymbol)
		}
		if future.ProviderInstrumentID != want.ProviderInstrumentID {
			t.Fatalf("futures uid %q is not the reviewed %q contract; a new contract requires the same live verification", future.ProviderInstrumentID, future.CanonicalSymbol)
		}
		if future.ExpirationUTCMS != want.ExpirationUTCMS {
			t.Fatalf("physical contract %q expiration %d drifted from the reviewed value %d", future.CanonicalSymbol, future.ExpirationUTCMS, want.ExpirationUTCMS)
		}
		if future.InstrumentType != model.InstrumentTypeFuture || future.ProviderIDKind != model.ProviderIDKindInstrumentUID || future.PriceUnit != model.PriceUnitPoints || future.VolumeUnit != model.VolumeUnitLots || future.Currency != "" {
			t.Fatalf("verified %q contract unit shape wrong: %+v", future.CanonicalSymbol, future)
		}
	}
	// The logical rolling alias targets exactly the reviewed active contract,
	// the provider ticker spelling resolves to the same contract, and the
	// physical uid resolves by itself (series identity).
	registry, err := tbankprovider.NewRegistry(tbankInstruments())
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	for _, future := range futures {
		want := verifiedFutures[future.CanonicalSymbol]
		for _, alias := range future.Aliases {
			if got, err := registry.Resolve(alias); err != nil || got.ProviderInstrumentID != want.ProviderInstrumentID {
				t.Fatalf("resolve %s = %+v err=%v", alias, got, err)
			}
		}
		if got, err := registry.Resolve(want.ProviderInstrumentID); err != nil || got.CanonicalSymbol != want.CanonicalSymbol {
			t.Fatalf("physical uid must resolve as identity: %+v err=%v", got, err)
		}
	}
}
