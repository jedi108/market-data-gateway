// Persistent registry source (F20): the refresh/restart strategy for the
// TBank instrument mapping, including rolling physical futures contracts.
//
// Refresh is an atomic full-snapshot replacement: the candidate list is
// validated first (an invalid list changes nothing), the validated snapshot is
// persisted with an atomic temp-file rename, and only then is the in-memory
// mapping swapped. Restart recovery reloads the persisted file; a missing file
// falls back to the compiled-in reviewed list, while a present-but-corrupt
// file fails closed — the gateway never starts with an unvalidated mapping.

package tbank

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// RegistrySchemaVersion is the schema version of the persisted instrument
// snapshot envelope. Bumping it is a breaking change to the file format and
// must fail-closed on read (see loadFile).
const RegistrySchemaVersion = 1

// persistedRegistry is the JSON envelope of one registry snapshot. The schema
// version makes the file self-describing so a future format change is detected
// at load, not silently misparsed.
type persistedRegistry struct {
	SchemaVersion int          `json:"schema_version"`
	Instruments   []Instrument `json:"instruments"`
}

// PersistedRegistry couples the in-memory registry with its persistent source.
// Refresh validates and persists before swapping, so the file on disk always
// holds a snapshot this binary can reload after a restart.
type PersistedRegistry struct {
	registry *Registry
	path     string
}

// OpenPersistedRegistry opens (and recovers) the registry from path. When the
// file does not exist the compiled-in fallback list seeds the registry (the
// pre-futures startup behavior for shares); when it exists it must parse and
// validate, otherwise startup fails closed. On success the caller owns a
// registry whose mapping survives restarts.
func OpenPersistedRegistry(path string, fallback []Instrument) (*PersistedRegistry, error) {
	instruments, existed, err := loadInstrumentsFile(path)
	if err != nil {
		return nil, fmt.Errorf("instrument registry file %s: %w", path, err)
	}
	if !existed {
		instruments = fallback
	}
	registry, err := NewRegistry(instruments)
	if err != nil {
		return nil, fmt.Errorf("instrument registry from %s: %w", path, err)
	}
	return &PersistedRegistry{registry: registry, path: path}, nil
}

// Registry exposes the live mapping for resolution, descriptors, and metrics.
func (p *PersistedRegistry) Registry() *Registry { return p.registry }

// Refresh applies an atomic mapping update: validate the candidate snapshot,
// persist it durably, then swap it into the registry. A failure at any step
// leaves the previously served mapping and the previously persisted file
// intact — a target switch is either fully applied or not applied at all.
func (p *PersistedRegistry) Refresh(instruments []Instrument) error {
	// Step 1: full validation without side effects.
	candidate, err := buildSnapshot(instruments)
	if err != nil {
		return err
	}
	// Step 2: durable persist of the validated snapshot (atomic rename).
	if err := saveInstrumentsFile(p.path, candidate.ordered); err != nil {
		return fmt.Errorf("persist instrument registry: %w", err)
	}
	// Step 3: atomic in-memory swap.
	return p.registry.Refresh(instruments)
}

func loadInstrumentsFile(path string) ([]Instrument, bool, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	var persisted persistedRegistry
	if err := json.Unmarshal(data, &persisted); err != nil {
		return nil, true, fmt.Errorf("corrupt registry file: %w", err)
	}
	if persisted.SchemaVersion != RegistrySchemaVersion {
		return nil, true, fmt.Errorf("unsupported registry schema version %d (supported %d); refusing to load", persisted.SchemaVersion, RegistrySchemaVersion)
	}
	return persisted.Instruments, true, nil
}

// saveInstrumentsFile writes the snapshot to a temp file in the same directory
// and renames it over the target, so a crash mid-write can never leave a
// partially written registry as the recovery source.
func saveInstrumentsFile(path string, instruments []Instrument) error {
	data, err := json.MarshalIndent(persistedRegistry{SchemaVersion: RegistrySchemaVersion, Instruments: instruments}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name) // no-op after a successful rename
	if _, err := temp.Write(append(data, '\n')); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
