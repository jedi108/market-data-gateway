package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/observability"
	tbankprovider "github.com/jedi108/market-data-gateway/internal/provider/tbank"
)

// refreshFileJSON renders a registry file payload for the refresh-trigger
// tests (same shape saveInstrumentsFile persists).
func refreshFileJSON(t *testing.T, instruments []tbankprovider.Instrument) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{"schema_version": tbankprovider.RegistrySchemaVersion, "instruments": instruments})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func goldDecemberActive() tbankprovider.Instrument {
	return tbankprovider.Instrument{CanonicalSymbol: "GOLD-26.12", ProviderInstrumentID: "synthetic-uid-gold-2612", MarketType: "futures", Aliases: []string{"GOLD"}, ExpirationUTCMS: 1797043200000}
}

func goldMarchActive() tbankprovider.Instrument {
	return tbankprovider.Instrument{CanonicalSymbol: "GOLD-27.03", ProviderInstrumentID: "synthetic-uid-gold-2703", MarketType: "futures", Aliases: []string{"GOLD"}, ExpirationUTCMS: 1809609600000}
}

// TestApplyRegistryRefreshReRecordsMetrics pins the composition-root half of
// the F20 runtime refresh: an applied reload re-records the registry gauge set
// (gateway_registry_* follow the served snapshot), and a rejected candidate
// leaves both the mapping and the metrics untouched.
func TestApplyRegistryRefreshReRecordsMetrics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	persisted, err := tbankprovider.OpenPersistedRegistry(path, []tbankprovider.Instrument{goldDecemberActive()})
	if err != nil {
		t.Fatal(err)
	}
	recorder := observability.NewRecorder()
	recorder.RecordRegistryState("tbank", tbankprovider.RegistrySchemaVersion, persisted.Registry().Refreshes(), registryInstrumentsForMetrics(persisted.Registry()))

	// No file yet: the tick is a no-op and changes no metric. Gauge symbols
	// are the bounded metric spellings (dots fold to dashes).
	applied, err := applyRegistryRefresh(persisted, recorder)
	if err != nil || applied {
		t.Fatalf("missing-file tick: applied=%t err=%v", applied, err)
	}
	assertRegistryMetrics(t, recorder, 0, "GOLD-26-12")

	// The operator rolls the contract in the file; the next tick applies it
	// and re-records the metrics from the new snapshot.
	if err := os.WriteFile(path, refreshFileJSON(t, []tbankprovider.Instrument{goldMarchActive()}), 0o600); err != nil {
		t.Fatal(err)
	}
	applied, err = applyRegistryRefresh(persisted, recorder)
	if err != nil || !applied {
		t.Fatalf("changed-file tick: applied=%t err=%v", applied, err)
	}
	assertRegistryMetrics(t, recorder, 1, "GOLD-27-03")

	// A corrupt file is rejected: mapping and metrics keep the last applied
	// state.
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"instruments":[`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := applyRegistryRefresh(persisted, recorder); err == nil {
		t.Fatal("corrupt file must be rejected")
	}
	assertRegistryMetrics(t, recorder, 1, "GOLD-27-03")
}

func assertRegistryMetrics(t *testing.T, recorder *observability.Recorder, wantRefreshes uint64, wantSymbol string) {
	t.Helper()
	snapshot := recorder.Snapshot()
	if snapshot.Registry == nil {
		t.Fatal("registry gauge set is not recorded")
	}
	if snapshot.Registry.Refreshes != wantRefreshes {
		t.Fatalf("gateway_registry_refreshes_total=%d want %d", snapshot.Registry.Refreshes, wantRefreshes)
	}
	found := false
	for _, gauge := range snapshot.Registry.Instruments {
		if gauge.Symbol == wantSymbol {
			found = true
			if gauge.MarketType != "futures" || gauge.InstrumentType != model.InstrumentTypeFuture {
				t.Fatalf("registry gauge for %q lost the futures identity: %+v", wantSymbol, gauge)
			}
			if gauge.ExpirationEpochSeconds == 0 {
				t.Fatalf("registry gauge for %q lost the contract expiry", wantSymbol)
			}
		}
	}
	if !found {
		t.Fatalf("registry gauge set does not contain %q: %+v", wantSymbol, snapshot.Registry.Instruments)
	}
}
