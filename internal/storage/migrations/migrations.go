// Package migrations owns the gateway database schema: an ordered, immutable
// chain of forward-only migrations. The production registry has a single
// current target (CurrentVersion); the storage layer drives it. Shipped
// migrations are frozen once applied anywhere: never edit an existing
// migration, only append the next version.
package migrations

import (
	"database/sql"
	"fmt"
)

// Executor abstracts the statement runner handed to a migration's Up: either
// the store's *sql.DB (schema completion) or the single *sql.Tx of one
// migration transaction.
type Executor interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// Migration is one immutable, forward-only schema step. Up must be written so
// that running it against an already-migrated database is a safe no-op:
// the storage layer re-applies the stored version's Up once per open to
// complete legacy databases that were stamped without their tables.
type Migration struct {
	Version int
	Name    string
	Up      func(exec Executor) error
}

// production is the single production migration chain. Migration 1 is the V1
// baseline exactly as originally shipped (series, candles, coverage and the
// candles_series_close_idx index). It must never be mutated in place.
var production = []Migration{
	{
		Version: 1,
		Name:    "v1_baseline_series_candles_coverage",
		Up:      upV1Baseline,
	},
}

// CurrentVersion is the schema version the production binary creates and
// migrates to. It is derived from the production registry, so it can never
// drift ahead of a shipped migration. The production binary never sees any
// other version: test-only extensions live exclusively in BuildTestRegistry.
func CurrentVersion() int {
	return production[len(production)-1].Version
}

// Migrations returns a copy of the production registry, ordered and
// contiguous from version 1. Callers cannot mutate the chain.
func Migrations() []Migration {
	chain := make([]Migration, len(production))
	copy(chain, production)
	return chain
}

// BuildTestRegistry is a test-only extension point: it returns a registry
// that extends the production chain with extra migrations simulating
// hypothetical future schema versions (for example a version 2). Extras must
// strictly extend the production chain. The production registry and
// CurrentVersion are never modified, and the production binary only ever
// opens through the storage layer's production Open, which uses Migrations():
// a test registry can therefore never reach a production binary.
func BuildTestRegistry(extra ...Migration) ([]Migration, error) {
	prodMax := production[len(production)-1].Version
	for _, m := range extra {
		if m.Version <= prodMax {
			return nil, fmt.Errorf("test migration %d does not extend the production chain (current version %d)", m.Version, prodMax)
		}
	}
	chain := make([]Migration, 0, len(production)+len(extra))
	chain = append(chain, production...)
	chain = append(chain, extra...)
	if err := Validate(chain); err != nil {
		return nil, err
	}
	return chain, nil
}

// Validate enforces the registry invariants the storage layer relies on:
// versions strictly contiguous from 1, every migration named, and every
// migration carrying an Up function.
func Validate(chain []Migration) error {
	for i, m := range chain {
		if m.Version != i+1 {
			return fmt.Errorf("migration registry must be contiguous from version 1: entry %d has version %d", i, m.Version)
		}
		if m.Name == "" {
			return fmt.Errorf("migration %d has no name", m.Version)
		}
		if m.Up == nil {
			return fmt.Errorf("migration %d has no Up function", m.Version)
		}
	}
	return nil
}

func upV1Baseline(exec Executor) error {
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS series (
            id INTEGER PRIMARY KEY,
            identity TEXT NOT NULL UNIQUE,
            venue TEXT NOT NULL,
            market_type TEXT NOT NULL,
            provider_instrument_id TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            candle_type TEXT NOT NULL,
            replica INTEGER NOT NULL DEFAULT 0 CHECK (replica IN (0, 1)),
            source_fetched_at_utc_ms INTEGER NOT NULL DEFAULT 0
        )`,
		`CREATE TABLE IF NOT EXISTS candles (
            series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
            open_time_utc_ms INTEGER NOT NULL,
            close_time_utc_ms INTEGER NOT NULL,
            open TEXT NOT NULL,
            high TEXT NOT NULL,
            low TEXT NOT NULL,
            close TEXT NOT NULL,
            volume TEXT NOT NULL,
            is_closed INTEGER NOT NULL CHECK (is_closed IN (0, 1)),
            PRIMARY KEY (series_id, open_time_utc_ms)
        )`,
		`CREATE INDEX IF NOT EXISTS candles_series_close_idx ON candles(series_id, close_time_utc_ms)`,
		`CREATE TABLE IF NOT EXISTS coverage (
            series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
            from_utc_ms INTEGER NOT NULL,
            to_utc_ms INTEGER NOT NULL CHECK (to_utc_ms > from_utc_ms),
            PRIMARY KEY (series_id, from_utc_ms, to_utc_ms)
        )`,
	} {
		if _, err := exec.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}
