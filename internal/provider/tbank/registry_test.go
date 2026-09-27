package tbank

import "testing"

func TestRegistryResolvesAliasesToOneInstrument(t *testing.T) {
	registry, err := NewRegistry([]Instrument{{CanonicalSymbol: "SBER", ProviderInstrumentID: "BBG004730N88", MarketType: "shares", Aliases: []string{"SBER/RUB", "BBG004730N88"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"SBER", "sber/rub", "BBG004730N88"} {
		got, err := registry.Resolve(alias)
		if err != nil || got.ProviderInstrumentID != "BBG004730N88" {
			t.Fatalf("Resolve(%q) = %#v, %v", alias, got, err)
		}
	}
}

func TestRegistryRejectsAmbiguousAlias(t *testing.T) {
	_, err := NewRegistry([]Instrument{{CanonicalSymbol: "A", ProviderInstrumentID: "1", MarketType: "shares", Aliases: []string{"X"}}, {CanonicalSymbol: "B", ProviderInstrumentID: "2", MarketType: "shares", Aliases: []string{"X"}}})
	if err == nil {
		t.Fatal("expected ambiguous alias error")
	}
}
