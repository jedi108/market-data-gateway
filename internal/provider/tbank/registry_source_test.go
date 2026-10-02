package tbank

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// persistedJSON renders a registry file the way saveInstrumentsFile does, so
// reload tests exercise the real file format.
func persistedJSON(t *testing.T, instruments []Instrument) []byte {
	t.Helper()
	data, err := json.Marshal(persistedRegistry{SchemaVersion: RegistrySchemaVersion, Instruments: instruments})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestPersistedRegistryReloadAppliesChangedSnapshotAndRewritesFile covers the
// runtime refresh trigger: a changed file is applied atomically (alias rolls
// to the new contract), the persisted file is rewritten with the applied
// snapshot (restart recovers it), and an unchanged file is a no-op.
func TestPersistedRegistryReloadAppliesChangedSnapshotAndRewritesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	if err := os.WriteFile(path, persistedJSON(t, []Instrument{goldDecemberActive()}), 0o600); err != nil {
		t.Fatal(err)
	}
	persisted, err := OpenPersistedRegistry(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := persisted.Registry().Resolve("GOLD"); err != nil || got.ProviderInstrumentID != "synthetic-uid-gold-2612" {
		t.Fatalf("seed mapping: %+v err=%v", got, err)
	}
	// The operator rolls the contract by updating the registry file.
	if err := os.WriteFile(path, persistedJSON(t, []Instrument{goldDecember(), goldMarchActive()}), 0o600); err != nil {
		t.Fatal(err)
	}
	applied, err := persisted.Reload()
	if err != nil || !applied {
		t.Fatalf("reload applied=%t err=%v", applied, err)
	}
	if got, err := persisted.Registry().Resolve("GOLD"); err != nil || got.ProviderInstrumentID != "synthetic-uid-gold-2703" {
		t.Fatalf("alias did not roll to the new contract: %+v err=%v", got, err)
	}
	if persisted.Registry().Refreshes() != 1 {
		t.Fatalf("refreshes=%d want 1", persisted.Registry().Refreshes())
	}
	// The applied snapshot is durably rewritten: restart recovery sees it.
	reopened, err := OpenPersistedRegistry(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Registry().Resolve("GOLD"); err != nil || got.ProviderInstrumentID != "synthetic-uid-gold-2703" {
		t.Fatalf("persisted file was not rewritten with the applied snapshot: %+v err=%v", got, err)
	}
	// The rewritten (normalized) file matches the served snapshot: no churn.
	applied, err = persisted.Reload()
	if err != nil || applied {
		t.Fatalf("second reload must be a no-op: applied=%t err=%v", applied, err)
	}
}

// TestPersistedRegistryReloadKeepsMappingOnInvalidSnapshot pins the fail-closed
// refresh semantics: a corrupt file and a parseable-but-invalid snapshot are
// both rejected without touching the served mapping or rewriting the file.
func TestPersistedRegistryReloadKeepsMappingOnInvalidSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	if err := os.WriteFile(path, persistedJSON(t, []Instrument{goldDecemberActive()}), 0o600); err != nil {
		t.Fatal(err)
	}
	persisted, err := OpenPersistedRegistry(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt file at runtime.
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"instruments":[`), 0o600); err != nil {
		t.Fatal(err)
	}
	applied, err := persisted.Reload()
	if err == nil || applied {
		t.Fatalf("corrupt file: applied=%t err=%v", applied, err)
	}
	if got, err := persisted.Registry().Resolve("GOLD"); err != nil || got.ProviderInstrumentID != "synthetic-uid-gold-2612" {
		t.Fatalf("mapping changed after a rejected refresh: %+v err=%v", got, err)
	}
	if persisted.Registry().Refreshes() != 0 {
		t.Fatalf("rejected refresh must not count: %d", persisted.Registry().Refreshes())
	}
	// Parseable but invalid snapshot (future without expiration).
	broken := goldContract("synthetic-uid-broken", "BROKEN", 0)
	if err := os.WriteFile(path, persistedJSON(t, []Instrument{broken}), 0o600); err != nil {
		t.Fatal(err)
	}
	applied, err = persisted.Reload()
	if err == nil || applied {
		t.Fatalf("invalid snapshot: applied=%t err=%v", applied, err)
	}
	if got, err := persisted.Registry().Resolve("GOLD"); err != nil || got.ProviderInstrumentID != "synthetic-uid-gold-2612" {
		t.Fatalf("mapping changed after an invalid snapshot: %+v err=%v", got, err)
	}
	// The rejected snapshot must not have been persisted back to the file.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(persistedJSON(t, []Instrument{broken})) {
		t.Fatal("file was rewritten by a rejected refresh")
	}
}

// TestPersistedRegistryReloadIgnoresCosmeticChanges proves the no-op path
// compares normalized snapshots: whitespace and value-case differences never
// trigger a rewrite.
func TestPersistedRegistryReloadIgnoresCosmeticChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	if err := os.WriteFile(path, persistedJSON(t, []Instrument{goldDecemberActive()}), 0o600); err != nil {
		t.Fatal(err)
	}
	persisted, err := OpenPersistedRegistry(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	cosmetic := goldDecemberActive()
	cosmetic.CanonicalSymbol = " GOLD-26.12 "
	cosmetic.MarketType = "FUTURES"
	cosmetic.Aliases = []string{" GOLD "}
	if err := os.WriteFile(path, persistedJSON(t, []Instrument{cosmetic}), 0o600); err != nil {
		t.Fatal(err)
	}
	applied, err := persisted.Reload()
	if err != nil || applied {
		t.Fatalf("cosmetic change must be a no-op: applied=%t err=%v", applied, err)
	}
	if persisted.Registry().Refreshes() != 0 {
		t.Fatalf("no-op reload must not count: %d", persisted.Registry().Refreshes())
	}
}

// TestPersistedRegistryReloadMissingFileIsNoOp pins that a deleted registry
// file never rolls the served mapping back at runtime.
func TestPersistedRegistryReloadMissingFileIsNoOp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	if err := os.WriteFile(path, persistedJSON(t, []Instrument{goldDecemberActive()}), 0o600); err != nil {
		t.Fatal(err)
	}
	persisted, err := OpenPersistedRegistry(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	applied, err := persisted.Reload()
	if err != nil || applied {
		t.Fatalf("missing file: applied=%t err=%v", applied, err)
	}
	if got, err := persisted.Registry().Resolve("GOLD"); err != nil || got.ProviderInstrumentID != "synthetic-uid-gold-2612" {
		t.Fatalf("mapping lost after the file disappeared: %+v err=%v", got, err)
	}
}

// TestVolatileRegistryRefusesRefresh pins that the compiled-in (no file)
// registry mode cannot be refreshed: without a persistent source there is no
// validated candidate and no recovery snapshot.
func TestVolatileRegistryRefusesRefresh(t *testing.T) {
	persisted, err := OpenVolatileRegistry([]Instrument{goldDecemberActive()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := persisted.Reload(); err == nil {
		t.Fatal("volatile reload must be refused")
	}
	if err := persisted.Refresh([]Instrument{goldMarchActive()}); err == nil {
		t.Fatal("volatile refresh must be refused")
	}
	if got, err := persisted.Registry().Resolve("GOLD"); err != nil || got.ProviderInstrumentID != "synthetic-uid-gold-2612" {
		t.Fatalf("mapping changed after refused refresh: %+v err=%v", got, err)
	}
}
