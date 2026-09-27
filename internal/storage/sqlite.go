package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/storage/migrations"

	_ "modernc.org/sqlite"
)

// Interval uses a half-open UTC millisecond range, matching the gateway
// coverage contract.
type Interval struct {
	FromUTCMS int64
	ToUTCMS   int64
}

type Metadata struct {
	Replica              bool
	SourceFetchedAtUTCMS int64
	Coverage             []Interval
}

type Snapshot struct {
	Candles  []model.Candle
	Metadata Metadata
}

// CleanupStats is the sanitized operational result of one retention pass.
// Counts are collected inside the same write transaction as the deletion so
// an operator can compare logical row retention before and after the pass.
type CleanupStats struct {
	CandlesBefore   int64
	CandlesAfter    int64
	CoverageBefore  int64
	CoverageAfter   int64
	DeletedCandles  int64
	DeletedCoverage int64
	TrimmedCoverage int64
	RemainingSeries int64
	Checkpoint      WALCheckpoint
}

// WALCheckpoint reports the result of PRAGMA wal_checkpoint(PASSIVE). A busy
// checkpoint is deliberately reported rather than escalated to TRUNCATE: a
// reader may safely defer physical WAL reclamation while logical retention is
// already bounded.
type WALCheckpoint struct {
	Busy         int64 `json:"busy"`
	LogFrames    int64 `json:"log_frames"`
	Checkpointed int64 `json:"checkpointed_frames"`
}

type Store struct {
	db          *sql.DB
	journalMode string
	foreignKeys bool
}

// Open opens the persistent store at path, creating or migrating its schema
// to the single production target migrations.CurrentVersion(). The target is
// chosen only inside the storage layer: callers cannot select a schema
// version. A newer on-disk schema than the binary supports is rejected
// fail-closed rather than migrated downward or destructively recreated.
func Open(path string) (*Store, error) {
	return openWithRegistry(path, migrations.Migrations())
}

// openWithRegistry drives a migration chain over the database. It is
// unexported so production callers can only ever reach the immutable
// production registry; tests use migrations.BuildTestRegistry to simulate
// hypothetical future schema versions.
func openWithRegistry(path string, chain []migrations.Migration) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)")
	if err != nil {
		return nil, err
	}
	// One connection serializes all statements: SQLite allows a single
	// writer, and a bounded pool prevents concurrent Save/Get traffic from
	// surfacing SQLITE_BUSY as request failures (task 109 cold start).
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.configure(chain); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) configure(chain []migrations.Migration) error {
	current, err := validateChain(chain)
	if err != nil {
		return err
	}
	if err := s.db.QueryRow("PRAGMA journal_mode").Scan(&s.journalMode); err != nil {
		return err
	}
	if strings.ToLower(s.journalMode) != "wal" {
		return fmt.Errorf("sqlite WAL mode was not enabled: %q", s.journalMode)
	}
	var foreignKeys int
	if err := s.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return err
	}
	s.foreignKeys = foreignKeys == 1
	if !s.foreignKeys {
		return fmt.Errorf("sqlite foreign keys were not enabled")
	}
	if _, err := s.db.Exec("CREATE TABLE IF NOT EXISTS gateway_schema (version INTEGER NOT NULL)"); err != nil {
		return err
	}
	var stored int
	// MAX(version): the table is kept single-row by applyMigration, but the
	// read stays deterministic even when facing a legacy or multi-row table.
	err = s.db.QueryRow("SELECT COALESCE(MAX(version), 0) FROM gateway_schema").Scan(&stored)
	if err == sql.ErrNoRows {
		stored = 0
	} else if err != nil {
		return err
	}
	if stored > current {
		return fmt.Errorf("schema version mismatch: stored=%d current=%d; refusing to migrate downward across an unknown schema", stored, current)
	}
	// Legacy completion: the pre-P1.3 probe wrote gateway_schema version 1
	// without the tables. The stored version's Up is idempotent, so
	// re-applying it safely completes such databases before any newer
	// migration runs on top.
	if stored > 0 {
		if err := chain[stored-1].Up(s.db); err != nil {
			return fmt.Errorf("complete schema version %d: %w", stored, err)
		}
	}
	for _, migration := range chain {
		if migration.Version <= stored {
			continue
		}
		if err := s.applyMigration(migration); err != nil {
			return err
		}
	}
	return nil
}

// validateChain enforces the registry invariants the driver relies on and
// returns the chain's current (highest) version.
func validateChain(chain []migrations.Migration) (int, error) {
	if len(chain) == 0 {
		return 0, fmt.Errorf("migration registry is empty")
	}
	if err := migrations.Validate(chain); err != nil {
		return 0, err
	}
	return chain[len(chain)-1].Version, nil
}

// applyMigration applies one forward migration in a single transaction: the
// schema statements and the gateway_schema version stamp commit together or
// not at all, so a failing migration leaves the database at the previous
// version.
func (s *Store) applyMigration(migration migrations.Migration) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := migration.Up(tx); err != nil {
		return fmt.Errorf("apply migration %d (%s): %w", migration.Version, migration.Name, err)
	}
	// Single-row invariant: the version marker holds exactly the current
	// on-disk schema version, never a history.
	if _, err := tx.Exec("DELETE FROM gateway_schema"); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO gateway_schema(version) VALUES(?)", migration.Version); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) JournalMode() string      { return s.journalMode }
func (s *Store) ForeignKeysEnabled() bool { return s.foreignKeys }
func (s *Store) Close() error             { return s.db.Close() }

// Save atomically merges candles and coverage. Empty provider results are a
// no-op so they cannot erase a last-known-good snapshot or mark a range covered.
func (s *Store) Save(key model.SeriesKey, snapshot Snapshot) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if len(snapshot.Candles) == 0 {
		return nil
	}
	for _, candle := range snapshot.Candles {
		if err := candle.Validate(); err != nil {
			return fmt.Errorf("invalid candle: %w", err)
		}
	}
	for _, interval := range snapshot.Metadata.Coverage {
		if interval.ToUTCMS <= interval.FromUTCMS {
			return fmt.Errorf("invalid coverage interval")
		}
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO series(identity, venue, market_type, provider_instrument_id, timeframe, candle_type, replica, source_fetched_at_utc_ms)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(identity) DO UPDATE SET
          replica = excluded.replica,
          source_fetched_at_utc_ms = MAX(series.source_fetched_at_utc_ms, excluded.source_fetched_at_utc_ms)`,
		key.Identity(), key.Venue, key.MarketType, key.ProviderInstrumentID, key.Timeframe, key.CandleType, boolInt(snapshot.Metadata.Replica), snapshot.Metadata.SourceFetchedAtUTCMS); err != nil {
		return err
	}
	var seriesID int64
	if err := tx.QueryRow("SELECT id FROM series WHERE identity = ?", key.Identity()).Scan(&seriesID); err != nil {
		return err
	}
	// Candle rows are inserted in multi-row batches: a cold start of dozens
	// of series otherwise spends most of its time in per-row statement
	// overhead while holding the single writer connection (task 109).
	const candleBatch = 32
	for start := 0; start < len(snapshot.Candles); start += candleBatch {
		end := start + candleBatch
		if end > len(snapshot.Candles) {
			end = len(snapshot.Candles)
		}
		batch := snapshot.Candles[start:end]
		var sb strings.Builder
		sb.WriteString(`INSERT INTO candles(series_id, open_time_utc_ms, close_time_utc_ms, open, high, low, close, volume, is_closed) VALUES `)
		args := make([]any, 0, len(batch)*9)
		for i, candle := range batch {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?)")
			args = append(args, seriesID, candle.OpenTimeUTCMS, candle.CloseTimeUTCMS, candle.Open, candle.High, candle.Low, candle.Close, candle.Volume, boolInt(candle.IsClosed))
		}
		sb.WriteString(` ON CONFLICT(series_id, open_time_utc_ms) DO UPDATE SET
              close_time_utc_ms=excluded.close_time_utc_ms, open=excluded.open, high=excluded.high,
              low=excluded.low, close=excluded.close, volume=excluded.volume, is_closed=excluded.is_closed
            WHERE NOT (candles.is_closed = 1 AND excluded.is_closed = 0)`)
		if _, err := tx.Exec(sb.String(), args...); err != nil {
			return err
		}
	}
	for _, interval := range snapshot.Metadata.Coverage {
		if _, err := tx.Exec("INSERT INTO coverage(series_id, from_utc_ms, to_utc_ms) VALUES(?, ?, ?) ON CONFLICT DO NOTHING", seriesID, interval.FromUTCMS, interval.ToUTCMS); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Get(key model.SeriesKey, fromUTCMS, toUTCMS int64) (Snapshot, bool, error) {
	if err := key.Validate(); err != nil {
		return Snapshot{}, false, err
	}
	if toUTCMS <= fromUTCMS {
		return Snapshot{}, false, fmt.Errorf("invalid request interval")
	}
	var seriesID int64
	var replica int
	var snapshot Snapshot
	err := s.db.QueryRow("SELECT id, replica, source_fetched_at_utc_ms FROM series WHERE identity = ?", key.Identity()).Scan(&seriesID, &replica, &snapshot.Metadata.SourceFetchedAtUTCMS)
	if err == sql.ErrNoRows {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, err
	}
	snapshot.Metadata.Replica = replica == 1
	rows, err := s.db.Query(`SELECT open_time_utc_ms, close_time_utc_ms, open, high, low, close, volume, is_closed
        FROM candles WHERE series_id = ? AND open_time_utc_ms >= ? AND close_time_utc_ms <= ? ORDER BY open_time_utc_ms`, seriesID, fromUTCMS, toUTCMS)
	if err != nil {
		return Snapshot{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var candle model.Candle
		var closed int
		if err := rows.Scan(&candle.OpenTimeUTCMS, &candle.CloseTimeUTCMS, &candle.Open, &candle.High, &candle.Low, &candle.Close, &candle.Volume, &closed); err != nil {
			return Snapshot{}, false, err
		}
		candle.IsClosed = closed == 1
		snapshot.Candles = append(snapshot.Candles, candle)
	}
	if err := rows.Err(); err != nil {
		return Snapshot{}, false, err
	}
	coverageRows, err := s.db.Query("SELECT from_utc_ms, to_utc_ms FROM coverage WHERE series_id = ? AND to_utc_ms > ? AND from_utc_ms < ? ORDER BY from_utc_ms", seriesID, fromUTCMS, toUTCMS)
	if err != nil {
		return Snapshot{}, false, err
	}
	defer coverageRows.Close()
	for coverageRows.Next() {
		var interval Interval
		if err := coverageRows.Scan(&interval.FromUTCMS, &interval.ToUTCMS); err != nil {
			return Snapshot{}, false, err
		}
		snapshot.Metadata.Coverage = append(snapshot.Metadata.Coverage, interval)
	}
	if err := coverageRows.Err(); err != nil {
		return Snapshot{}, false, err
	}
	return snapshot, len(snapshot.Candles) > 0, nil
}

// StaleIfUpstreamError makes stale delivery explicit: callers must provide the
// upstream error and a policy-derived maximum age. It never labels data fresh.
func (s *Store) StaleIfUpstreamError(key model.SeriesKey, fromUTCMS, toUTCMS, nowUTCMS, maxAgeMS int64, upstreamErr error) (Snapshot, bool, error) {
	if upstreamErr == nil {
		return Snapshot{}, false, nil
	}
	if maxAgeMS < 0 {
		return Snapshot{}, false, fmt.Errorf("stale maximum age must not be negative")
	}
	snapshot, found, err := s.Get(key, fromUTCMS, toUTCMS)
	if err != nil || !found || snapshot.Metadata.SourceFetchedAtUTCMS <= 0 {
		return snapshot, false, err
	}
	age := nowUTCMS - snapshot.Metadata.SourceFetchedAtUTCMS
	return snapshot, age >= 0 && age <= maxAgeMS, nil
}

// CleanupBefore enforces bounded retention while retaining each series' latest
// closed candle, which remains the potential stale copy for that series. It is
// kept as a compatibility wrapper for existing callers and tests.
func (s *Store) CleanupBefore(cutoffUTCMS int64) (int64, error) {
	stats, err := s.CleanupBeforeWithStats(cutoffUTCMS)
	return stats.DeletedCandles, err
}

// CleanupBeforeWithStats performs one retention pass under an immediate
// SQLite write transaction. Forming/presentation-only candles are never
// deleted by retention; expired closed candles are removed except for the
// latest closed candle of every series. Coverage is deleted when wholly
// expired and left-trimmed when it crosses the cutoff, so it never points at
// a deleted historical range.
func (s *Store) CleanupBeforeWithStats(cutoffUTCMS int64) (CleanupStats, error) {
	var stats CleanupStats
	ctx := context.Background()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return stats, err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return stats, fmt.Errorf("begin cleanup transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM candles").Scan(&stats.CandlesBefore); err != nil {
		return stats, err
	}
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM coverage").Scan(&stats.CoverageBefore); err != nil {
		return stats, err
	}

	result, err := conn.ExecContext(ctx, `DELETE FROM candles AS old
		WHERE old.is_closed = 1
		  AND old.close_time_utc_ms < ?
		  AND old.close_time_utc_ms < (SELECT MAX(current.close_time_utc_ms) FROM candles AS current WHERE current.series_id = old.series_id AND current.is_closed = 1)`, cutoffUTCMS)
	if err != nil {
		return stats, fmt.Errorf("delete expired candles: %w", err)
	}
	stats.DeletedCandles, err = result.RowsAffected()
	if err != nil {
		return stats, fmt.Errorf("count deleted candles: %w", err)
	}

	// Read coverage rows first, then mutate them after closing the cursor. The
	// delete+insert for a crossing interval also handles an existing identical
	// trimmed row without creating a duplicate primary key.
	rows, err := conn.QueryContext(ctx, `SELECT series_id, from_utc_ms, to_utc_ms
		FROM coverage WHERE from_utc_ms < ? ORDER BY series_id, from_utc_ms, to_utc_ms`, cutoffUTCMS)
	if err != nil {
		return stats, fmt.Errorf("scan coverage: %w", err)
	}
	type coverageRow struct {
		seriesID int64
		from     int64
		to       int64
	}
	var affected []coverageRow
	for rows.Next() {
		var row coverageRow
		if err := rows.Scan(&row.seriesID, &row.from, &row.to); err != nil {
			rows.Close()
			return stats, fmt.Errorf("scan coverage row: %w", err)
		}
		affected = append(affected, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return stats, fmt.Errorf("scan coverage rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return stats, fmt.Errorf("close coverage rows: %w", err)
	}
	for _, row := range affected {
		if row.to <= cutoffUTCMS {
			result, err := conn.ExecContext(ctx, `DELETE FROM coverage
				WHERE series_id = ? AND from_utc_ms = ? AND to_utc_ms = ?`, row.seriesID, row.from, row.to)
			if err != nil {
				return stats, fmt.Errorf("delete expired coverage: %w", err)
			}
			deleted, err := result.RowsAffected()
			if err != nil {
				return stats, fmt.Errorf("count deleted coverage: %w", err)
			}
			stats.DeletedCoverage += deleted
			continue
		}
		_, err := conn.ExecContext(ctx, `DELETE FROM coverage
			WHERE series_id = ? AND from_utc_ms = ? AND to_utc_ms = ?`, row.seriesID, row.from, row.to)
		if err != nil {
			return stats, fmt.Errorf("replace crossing coverage: %w", err)
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO coverage(series_id, from_utc_ms, to_utc_ms)
			VALUES(?, ?, ?) ON CONFLICT DO NOTHING`, row.seriesID, cutoffUTCMS, row.to); err != nil {
			return stats, fmt.Errorf("insert trimmed coverage: %w", err)
		}
		stats.TrimmedCoverage++
	}

	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM candles").Scan(&stats.CandlesAfter); err != nil {
		return stats, err
	}
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM coverage").Scan(&stats.CoverageAfter); err != nil {
		return stats, err
	}
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM series").Scan(&stats.RemainingSeries); err != nil {
		return stats, err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return stats, fmt.Errorf("commit cleanup transaction: %w", err)
	}
	committed = true

	if err := conn.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&stats.Checkpoint.Busy, &stats.Checkpoint.LogFrames, &stats.Checkpoint.Checkpointed); err != nil {
		return stats, fmt.Errorf("checkpoint WAL: %w", err)
	}
	return stats, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
