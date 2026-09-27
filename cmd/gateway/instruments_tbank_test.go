package main

import (
	"testing"

	tbankprovider "github.com/jedi108/market-data-gateway/internal/provider/tbank"
)

// reviewedWhitelist pins the expected 40-symbol universe served by the
// registry. AFR is not part of it: TBank does not list the instrument at all.
var reviewedWhitelist = []string{
	"GAZP", "LKOH", "YDEX", "SBER", "VTB", "MOEX", "IRAO", "NLMK", "GCHE",
	"RASP", "HEAD", "VKCO", "ROSN", "ASTR", "AFLT", "FLOT", "MAGN",
	"POSI", "MTSS", "RUAL", "OZON", "T", "X5", "MGNT", "SMLT", "ALRS",
	"PLZL", "SIBN", "TATN", "NVTK", "GMKN", "SGZH", "UPRO", "SPBE", "AFKS",
	"CHMF", "RNFT", "RAGR", "SFIN", "SVCB",
}

func TestTbankInstrumentsBuildValidRegistry(t *testing.T) {
	registry, err := tbankprovider.NewRegistry(tbankInstruments())
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	if len(tbankInstruments()) != 40 {
		t.Fatalf("instruments=%d, want 40 (the reviewed universe; AFR delisted)", len(tbankInstruments()))
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
