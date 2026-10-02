package tbank

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// Synthetic offline futures instruments only: no real TBank physical GOLD UID
// exists offline (F20 online DoD adds the reviewed real ones), so tests pin
// the identity model with synthetic UIDs.
//
// In one snapshot the logical alias "GOLD" is declared on exactly the ACTIVE
// contract; the rolled-from contract stays listed WITHOUT the alias so its
// physical UID (and cached history) remains reachable while the alias points
// elsewhere. Declaring "GOLD" on both contracts is the ambiguous-alias
// configuration error and fails closed.

// goldContract builds a synthetic physical GOLD contract.
func goldContract(uid, symbol string, expirationUTCMS int64, aliases ...string) Instrument {
	return Instrument{CanonicalSymbol: symbol, ProviderInstrumentID: uid, MarketType: "futures", Aliases: aliases, ExpirationUTCMS: expirationUTCMS}
}

// goldDecemberActive is the December contract targeted by the logical alias.
func goldDecemberActive() Instrument {
	return goldContract("synthetic-uid-gold-2612", "GOLD-26.12", 1797043200000, "GOLD")
}

// goldDecember is the December contract after the roll: no logical alias.
func goldDecember() Instrument {
	return goldContract("synthetic-uid-gold-2612", "GOLD-26.12", 1797043200000)
}

// goldMarch is the March contract before the roll targets it.
func goldMarch() Instrument {
	return goldContract("synthetic-uid-gold-2703", "GOLD-27.03", 1809609600000)
}

// goldMarchActive is the March contract targeted by the logical alias.
func goldMarchActive() Instrument {
	return goldContract("synthetic-uid-gold-2703", "GOLD-27.03", 1809609600000, "GOLD")
}

func TestRegistryFuturesExpiriesAreDistinctSeriesIdentities(t *testing.T) {
	registry, err := NewRegistry([]Instrument{goldDecemberActive(), goldMarch()})
	if err != nil {
		t.Fatal(err)
	}
	// The logical alias resolves to exactly one physical contract per
	// snapshot; both physical UIDs resolve to themselves.
	active, err := registry.Resolve("GOLD")
	if err != nil {
		t.Fatal(err)
	}
	december, ok := registry.InstrumentByProviderID("synthetic-uid-gold-2612")
	if !ok || december.CanonicalSymbol != "GOLD-26.12" {
		t.Fatalf("physical UID lookup: %+v ok=%t", december, ok)
	}
	march, ok := registry.InstrumentByProviderID("synthetic-uid-gold-2703")
	if !ok || march.CanonicalSymbol != "GOLD-27.03" {
		t.Fatalf("physical UID lookup: %+v ok=%t", march, ok)
	}
	if active.ProviderInstrumentID != december.ProviderInstrumentID {
		t.Fatalf("logical alias must resolve to the active December contract: %+v", active)
	}
	keyDecember := model.SeriesKey{Venue: "tbank", MarketType: december.MarketType, ProviderInstrumentID: december.ProviderInstrumentID, Timeframe: "1h", CandleType: "trade"}
	keyMarch := model.SeriesKey{Venue: "tbank", MarketType: march.MarketType, ProviderInstrumentID: march.ProviderInstrumentID, Timeframe: "1h", CandleType: "trade"}
	if keyDecember.Identity() == keyMarch.Identity() {
		t.Fatal("different expiries must produce different series identities")
	}
}

func TestRegistryFuturesDefaultsCarryExplicitUnits(t *testing.T) {
	registry, err := NewRegistry([]Instrument{goldDecemberActive(), {CanonicalSymbol: "SBER", ProviderInstrumentID: "BBG004730N88", MarketType: "shares"}})
	if err != nil {
		t.Fatal(err)
	}
	gold, err := registry.Resolve("gold") // case-insensitive alias
	if err != nil {
		t.Fatal(err)
	}
	if gold.InstrumentType != model.InstrumentTypeFuture || gold.PriceUnit != model.PriceUnitPoints || gold.ProviderIDKind != model.ProviderIDKindInstrumentUID || gold.VolumeUnit != model.VolumeUnitLots || gold.ExpirationUTCMS == 0 || gold.Currency != "" {
		t.Fatalf("futures unit defaults: %+v", gold)
	}
	metadata, ok := registry.Describe(model.SeriesKey{Venue: "tbank", MarketType: "futures", ProviderInstrumentID: gold.ProviderInstrumentID, Timeframe: "1h", CandleType: "trade"})
	if !ok {
		t.Fatal("describe futures series")
	}
	if metadata.InstrumentType != model.InstrumentTypeFuture || metadata.PriceUnit != model.PriceUnitPoints || metadata.VolumeUnit != model.VolumeUnitLots || metadata.ExpirationUTCMS != gold.ExpirationUTCMS || metadata.CanonicalSymbol != "GOLD-26.12" {
		t.Fatalf("futures response metadata: %+v", metadata)
	}
	// Share defaults keep the legacy shape and stay describe-able.
	sber, err := registry.Resolve("SBER")
	if err != nil {
		t.Fatal(err)
	}
	if sber.InstrumentType != model.InstrumentTypeShare || sber.PriceUnit != model.PriceUnitCurrency || sber.ProviderIDKind != model.ProviderIDKindFIGI || sber.ExpirationUTCMS != 0 {
		t.Fatalf("share unit defaults: %+v", sber)
	}
}

func TestRegistryRefreshRollsLogicalAliasAtomically(t *testing.T) {
	registry, err := NewRegistry([]Instrument{goldDecemberActive()})
	if err != nil {
		t.Fatal(err)
	}
	// An invalid refresh is rejected and changes nothing (fail closed).
	if err := registry.Refresh([]Instrument{{CanonicalSymbol: "BROKEN", ProviderInstrumentID: "", MarketType: "futures"}}); err == nil {
		t.Fatal("invalid refresh must fail")
	}
	if got, err := registry.Resolve("GOLD"); err != nil || got.ProviderInstrumentID != "synthetic-uid-gold-2612" {
		t.Fatalf("failed refresh changed the mapping: %+v err=%v", got, err)
	}
	if registry.Refreshes() != 0 {
		t.Fatalf("failed refresh must not count: %d", registry.Refreshes())
	}
	// A valid refresh rolls the logical alias to the March contract while the
	// December physical UID keeps its own identity and remains resolvable by
	// UID (its cached history stays reachable).
	if err := registry.Refresh([]Instrument{goldDecember(), goldMarchActive()}); err != nil {
		t.Fatal(err)
	}
	if got, err := registry.Resolve("GOLD"); err != nil || got.ProviderInstrumentID != "synthetic-uid-gold-2703" {
		t.Fatalf("alias did not roll to the new target: %+v err=%v", got, err)
	}
	if got, err := registry.Resolve("synthetic-uid-gold-2612"); err != nil || got.CanonicalSymbol != "GOLD-26.12" {
		t.Fatalf("old physical UID must stay resolvable: %+v err=%v", got, err)
	}
	if registry.Refreshes() != 1 {
		t.Fatalf("refreshes=%d want 1", registry.Refreshes())
	}
}

func TestRegistryConcurrentResolveDuringRefresh(t *testing.T) {
	registry, err := NewRegistry([]Instrument{goldDecemberActive(), goldMarch()})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	unexpected := make(chan string, 64)
	for i := range 8 {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < 64; j++ {
				got, err := registry.Resolve("GOLD")
				if err != nil {
					unexpected <- "resolve error: " + err.Error()
					return
				}
				if got.ProviderInstrumentID != "synthetic-uid-gold-2612" && got.ProviderInstrumentID != "synthetic-uid-gold-2703" {
					unexpected <- "torn snapshot: " + got.ProviderInstrumentID
					return
				}
				if seed == 0 && j%16 == 0 {
					// One writer keeps swapping which physical contract the
					// alias points at; readers must always observe one of the
					// two complete snapshots, never a mix.
					var target []Instrument
					if (j/16)%2 == 0 {
						target = []Instrument{goldDecember(), goldMarchActive()}
					} else {
						target = []Instrument{goldDecemberActive(), goldMarch()}
					}
					if err := registry.Refresh(target); err != nil {
						unexpected <- "refresh error: " + err.Error()
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
	close(unexpected)
	for problem := range unexpected {
		t.Fatal(problem)
	}
}

func TestRegistryRejectsUnsupportedInstrumentType(t *testing.T) {
	_, err := NewRegistry([]Instrument{{CanonicalSymbol: "OPT", ProviderInstrumentID: "uid-opt", MarketType: "options", InstrumentType: "option"}})
	if err == nil {
		t.Fatal("unsupported instrument type must fail closed")
	}
	var typed *apperror.Error
	if !errors.As(err, &typed) || typed.Code != apperror.CodeUnsupportedCapability {
		t.Fatalf("expected UNSUPPORTED_CAPABILITY, got %v", err)
	}
}

func TestRegistryFuturesValidationNegativePaths(t *testing.T) {
	cases := map[string][]Instrument{
		"future requires expiration": {goldContract("synthetic-uid", "GOLD-26.12", 0, "GOLD")},
		"future must not claim a currency": func() []Instrument {
			instrument := goldContract("synthetic-uid", "GOLD-26.12", 1797043200000, "GOLD")
			instrument.Currency = "RUB"
			return []Instrument{instrument}
		}(),
		"future must use the futures market": func() []Instrument {
			instrument := goldContract("synthetic-uid", "GOLD-26.12", 1797043200000, "GOLD")
			instrument.MarketType = "shares"
			return []Instrument{instrument}
		}(),
		"share must not use the futures market": {
			{CanonicalSymbol: "SBER", ProviderInstrumentID: "BBG004730N88", MarketType: "futures"},
		},
		"share must not carry an expiration": {
			{CanonicalSymbol: "SBER", ProviderInstrumentID: "BBG004730N88", MarketType: "shares", ExpirationUTCMS: 1},
		},
		"duplicate physical uid":              {goldDecember(), goldDecember()},
		"alias on two contracts is ambiguous": {goldDecemberActive(), goldMarchActive()},
	}
	for name, instruments := range cases {
		if _, err := NewRegistry(instruments); err == nil {
			t.Fatalf("%s: expected registry construction failure", name)
		}
	}
}

func TestCheckCapabilityBoundary(t *testing.T) {
	if err := CheckCapability(CapabilityCandles); err != nil {
		t.Fatalf("candles capability must be served: %v", err)
	}
	for _, capability := range []string{"orderbook", "trades_stream", "last_price", "orders"} {
		err := CheckCapability(capability)
		if err == nil {
			t.Fatalf("capability %q must fail closed", capability)
		}
		var typed *apperror.Error
		if !errors.As(err, &typed) || typed.Code != apperror.CodeUnsupportedCapability {
			t.Fatalf("capability %q: expected UNSUPPORTED_CAPABILITY, got %v", capability, err)
		}
	}
	if strings.Join(SupportedCapabilities(), ",") != CapabilityCandles {
		t.Fatalf("served capabilities drifted: %v", SupportedCapabilities())
	}
}

func TestPersistedRegistryRefreshSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	persisted, err := OpenPersistedRegistry(path, []Instrument{goldDecemberActive()})
	if err != nil {
		t.Fatal(err)
	}
	// Target switch: the alias rolls to March, both physical contracts stay.
	if err := persisted.Refresh([]Instrument{goldDecember(), goldMarchActive()}); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPersistedRegistry(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	registry := reopened.Registry()
	if got, err := registry.Resolve("GOLD"); err != nil || got.ProviderInstrumentID != "synthetic-uid-gold-2703" {
		t.Fatalf("restart did not recover the refreshed mapping: %+v err=%v", got, err)
	}
	if got, err := registry.Resolve("synthetic-uid-gold-2612"); err != nil || got.CanonicalSymbol != "GOLD-26.12" {
		t.Fatalf("restart lost the old physical identity: %+v err=%v", got, err)
	}
}

func TestPersistedRegistryMissingFileFallsBackToCompiledList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.json")
	persisted, err := OpenPersistedRegistry(path, []Instrument{{CanonicalSymbol: "SBER", ProviderInstrumentID: "BBG004730N88", MarketType: "shares"}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := persisted.Registry().Resolve("SBER"); err != nil || got.ProviderInstrumentID != "BBG004730N88" {
		t.Fatalf("fallback list not applied: %+v err=%v", got, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("merely opening must not write the registry file")
	}
}

func TestPersistedRegistryCorruptFileFailsClosed(t *testing.T) {
	cases := map[string]string{
		"invalid json":       `{"schema_version":1,"instruments":[`,
		"future schema":      `{"schema_version":99,"instruments":[]}`,
		"invalid instrument": `{"schema_version":1,"instruments":[{"canonical_symbol":"GOLD","provider_instrument_id":"uid","market_type":"futures"}]}`,
	}
	for name, content := range cases {
		path := filepath.Join(t.TempDir(), "registry.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenPersistedRegistry(path, nil); err == nil {
			t.Fatalf("%s: corrupt registry file must fail closed", name)
		}
	}
}
