package storage

import (
	"errors"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/storage/migrations"
)

func testKey() model.SeriesKey {
	return model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "figi", Timeframe: "1m", CandleType: "trade"}
}

func testCandle(open int64, closed bool) model.Candle {
	return model.Candle{OpenTimeUTCMS: open, CloseTimeUTCMS: open + 60, Open: "1", High: "2", Low: "1", Close: "2", Volume: "0", IsClosed: closed}
}

func TestOpenInitializesWALAndCreatesCurrentSchema(t *testing.T) {
	path := t.TempDir() + "/cache.sqlite"
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if store.JournalMode() != "wal" || !store.ForeignKeysEnabled() {
		t.Fatalf("sqlite pragmas were not enabled: journal=%q foreign_keys=%t", store.JournalMode(), store.ForeignKeysEnabled())
	}
	var tableCount int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('series','candles','coverage','gateway_schema')").Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 4 {
		t.Fatalf("fresh database did not receive the full current schema: %d/4 tables", tableCount)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Repeated open of a current-version database is idempotent.
	store, err = Open(path)
	if err != nil {
		t.Fatalf("repeated open of a current database failed: %v", err)
	}
	var stored int
	if err := store.db.QueryRow("SELECT version FROM gateway_schema LIMIT 1").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != migrations.CurrentVersion() {
		t.Fatalf("stored schema version drifted: stored=%d current=%d", stored, migrations.CurrentVersion())
	}
	store.Close()
}

func TestSaveSurvivesRestartAndPreservesCoverageAndReplica(t *testing.T) {
	path := t.TempDir() + "/cache.sqlite"
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{Candles: []model.Candle{testCandle(0, true), testCandle(60, false)}, Metadata: Metadata{Replica: true, SourceFetchedAtUTCMS: 120, Coverage: []Interval{{FromUTCMS: 0, ToUTCMS: 120}}}}
	if err := store.Save(testKey(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, found, err := store.Get(testKey(), 0, 120)
	if err != nil || !found {
		t.Fatalf("restart load: found=%t err=%v", found, err)
	}
	if len(got.Candles) != 2 || !got.Metadata.Replica || got.Metadata.SourceFetchedAtUTCMS != 120 || len(got.Metadata.Coverage) != 1 {
		t.Fatalf("unexpected persisted snapshot: %+v", got)
	}
}

func TestLegacyV1DatabaseIsSafelyCompleted(t *testing.T) {
	path := t.TempDir() + "/legacy.sqlite"
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("DROP TABLE candles"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("DROP TABLE coverage"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("DROP TABLE series"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(testKey(), Snapshot{Candles: []model.Candle{testCandle(0, true)}}); err != nil {
		t.Fatalf("legacy schema was not safely completed: %v", err)
	}
}

// TestFailedMigrationLeavesPreviousVersion proves migration atomicity with a
// test-only registry: a failing migration must leave the database exactly at
// the previous version — no partial schema, no version stamp — so the
// production binary can still open it afterwards.
func TestFailedMigrationLeavesPreviousVersion(t *testing.T) {
	path := t.TempDir() + "/atomic.sqlite"
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	chain, err := migrations.BuildTestRegistry(migrations.Migration{
		Version: 2,
		Name:    "v2_test_failing",
		Up: func(exec migrations.Executor) error {
			if _, err := exec.Exec("CREATE TABLE v2_partial (id INTEGER PRIMARY KEY)"); err != nil {
				return err
			}
			return errors.New("simulated migration failure")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openWithRegistry(path, chain); err == nil {
		t.Fatal("expected failing migration rejection")
	}

	// The database must be untouched at version 1: production Open succeeds
	// and no partial V2 object exists.
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("database was not left at the previous version: %v", err)
	}
	defer reopened.Close()
	var stored int
	if err := reopened.db.QueryRow("SELECT version FROM gateway_schema LIMIT 1").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != migrations.CurrentVersion() {
		t.Fatalf("version stamp moved on failed migration: stored=%d current=%d", stored, migrations.CurrentVersion())
	}
	var partial int
	if err := reopened.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='v2_partial'").Scan(&partial); err != nil {
		t.Fatal(err)
	}
	if partial != 0 {
		t.Fatal("failed migration left partial schema objects behind")
	}
}

func TestSaveDoesNotRegressClosedCandleOrEraseOnEmpty(t *testing.T) {
	store, err := Open(t.TempDir() + "/cache.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(testKey(), Snapshot{Candles: []model.Candle{testCandle(0, true)}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(testKey(), Snapshot{Candles: []model.Candle{testCandle(0, false)}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(testKey(), Snapshot{}); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Get(testKey(), 0, 60)
	if err != nil || !found || len(got.Candles) != 1 || !got.Candles[0].IsClosed {
		t.Fatalf("closed candle regressed or was erased: %+v found=%t err=%v", got, found, err)
	}
}

func TestCleanupRetainsOnlyStaleFallbackCandle(t *testing.T) {
	store, err := Open(t.TempDir() + "/cache.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(testKey(), Snapshot{Candles: []model.Candle{testCandle(0, true), testCandle(60, true), testCandle(120, true)}}); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.CleanupBefore(180)
	if err != nil || deleted != 2 {
		t.Fatalf("cleanup deleted=%d err=%v", deleted, err)
	}
	got, found, err := store.Get(testKey(), 0, 180)
	if err != nil || !found || len(got.Candles) != 1 || got.Candles[0].OpenTimeUTCMS != 120 {
		t.Fatalf("latest stale fallback was not retained: %+v found=%t err=%v", got, found, err)
	}
}

func TestCleanupTrimsCoverageAndPreservesClosedAndFormingRows(t *testing.T) {
	store, err := Open(t.TempDir() + "/cache.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	forming := testCandle(120, false)
	if err := store.Save(testKey(), Snapshot{
		Candles:  []model.Candle{testCandle(0, true), testCandle(60, true), forming},
		Metadata: Metadata{Coverage: []Interval{{FromUTCMS: 0, ToUTCMS: 240}}},
	}); err != nil {
		t.Fatal(err)
	}
	other := model.SeriesKey{Venue: "tbank", MarketType: "shares", ProviderInstrumentID: "other", Timeframe: "1m", CandleType: "trade"}
	if err := store.Save(other, Snapshot{
		Candles:  []model.Candle{testCandle(0, true)},
		Metadata: Metadata{Coverage: []Interval{{FromUTCMS: 0, ToUTCMS: 60}}},
	}); err != nil {
		t.Fatal(err)
	}

	stats, err := store.CleanupBeforeWithStats(180)
	if err != nil {
		t.Fatal(err)
	}
	if stats.CandlesBefore != 4 || stats.CandlesAfter != 3 || stats.DeletedCandles != 1 {
		t.Fatalf("unexpected candle cleanup stats: %+v", stats)
	}
	if stats.CoverageBefore != 2 || stats.CoverageAfter != 1 || stats.DeletedCoverage != 1 || stats.TrimmedCoverage != 1 {
		t.Fatalf("unexpected coverage cleanup stats: %+v", stats)
	}
	if stats.RemainingSeries != 2 || stats.Checkpoint.Busy != 0 {
		t.Fatalf("unexpected operational stats: %+v", stats)
	}

	got, found, err := store.Get(testKey(), 0, 240)
	if err != nil || !found || len(got.Candles) != 2 {
		t.Fatalf("closed/forming rows were not preserved: snapshot=%+v found=%t err=%v", got, found, err)
	}
	if got.Candles[0].OpenTimeUTCMS != 60 || !got.Candles[0].IsClosed || got.Candles[1].OpenTimeUTCMS != 120 || got.Candles[1].IsClosed {
		t.Fatalf("wrong retained candles: %+v", got.Candles)
	}
	if len(got.Metadata.Coverage) != 1 || got.Metadata.Coverage[0] != (Interval{FromUTCMS: 180, ToUTCMS: 240}) {
		t.Fatalf("coverage was not left-trimmed: %+v", got.Metadata.Coverage)
	}

	second, err := store.CleanupBeforeWithStats(180)
	if err != nil {
		t.Fatal(err)
	}
	if second.DeletedCandles != 0 || second.DeletedCoverage != 0 || second.TrimmedCoverage != 0 || second.CandlesAfter != stats.CandlesAfter || second.CoverageAfter != stats.CoverageAfter {
		t.Fatalf("cleanup is not idempotent: first=%+v second=%+v", stats, second)
	}
}

func TestStaleIfUpstreamErrorRequiresErrorAndAgePolicy(t *testing.T) {
	store, err := Open(t.TempDir() + "/cache.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(testKey(), Snapshot{Candles: []model.Candle{testCandle(0, true)}, Metadata: Metadata{Replica: true, SourceFetchedAtUTCMS: 100}}); err != nil {
		t.Fatal(err)
	}
	if _, allowed, err := store.StaleIfUpstreamError(testKey(), 0, 60, 150, 100, nil); err != nil || allowed {
		t.Fatalf("stale without upstream error: allowed=%t err=%v", allowed, err)
	}
	got, allowed, err := store.StaleIfUpstreamError(testKey(), 0, 60, 150, 50, errors.New("provider unavailable"))
	if err != nil || !allowed || !got.Metadata.Replica {
		t.Fatalf("expected explicit replica stale fallback: snapshot=%+v allowed=%t err=%v", got, allowed, err)
	}
	if _, allowed, err := store.StaleIfUpstreamError(testKey(), 0, 60, 151, 50, errors.New("provider unavailable")); err != nil || allowed {
		t.Fatalf("expired stale fallback: allowed=%t err=%v", allowed, err)
	}
}

// futuresKey is a physical futures series key: the logical "GOLD" alias never
// appears here — identity is the per-expiry provider instrument UID.
func futuresKey(uid string) model.SeriesKey {
	return model.SeriesKey{Venue: "tbank", MarketType: "futures", ProviderInstrumentID: uid, Timeframe: "1h", CandleType: "trade"}
}

// TestFuturesExpiriesAreDistinctCacheEntries proves the F20 identity contract
// at the durable layer: two expiries of one logical future are two series, and
// writing the new contract never touches the old contract's history.
func TestFuturesExpiriesAreDistinctCacheEntries(t *testing.T) {
	store, err := Open(t.TempDir() + "/futures.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	december, march := futuresKey("synthetic-uid-gold-2612"), futuresKey("synthetic-uid-gold-2703")
	if december.Identity() == march.Identity() {
		t.Fatal("different expiries must produce different series identities")
	}
	if err := store.Save(december, Snapshot{Candles: []model.Candle{testCandle(0, true), testCandle(60, true)}, Metadata: Metadata{SourceFetchedAtUTCMS: 100, Coverage: []Interval{{FromUTCMS: 0, ToUTCMS: 120}}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(march, Snapshot{Candles: []model.Candle{testCandle(0, true)}, Metadata: Metadata{SourceFetchedAtUTCMS: 200, Coverage: []Interval{{FromUTCMS: 0, ToUTCMS: 60}}}}); err != nil {
		t.Fatal(err)
	}
	var seriesCount int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM series WHERE market_type = 'futures'").Scan(&seriesCount); err != nil {
		t.Fatal(err)
	}
	if seriesCount != 2 {
		t.Fatalf("futures series rows=%d want 2", seriesCount)
	}
	// Target switch simulation: a later refresh writing only the new active
	// contract must leave the old contract's rows byte-identical.
	if err := store.Save(march, Snapshot{Candles: []model.Candle{testCandle(60, true), testCandle(120, true)}, Metadata: Metadata{SourceFetchedAtUTCMS: 300, Coverage: []Interval{{FromUTCMS: 60, ToUTCMS: 180}}}}); err != nil {
		t.Fatal(err)
	}
	old, found, err := store.Get(december, 0, 120)
	if err != nil || !found {
		t.Fatalf("old contract history: found=%t err=%v", found, err)
	}
	if len(old.Candles) != 2 || old.Metadata.SourceFetchedAtUTCMS != 100 {
		t.Fatalf("old contract history was rewritten: %+v", old)
	}
	updated, found, err := store.Get(march, 0, 180)
	if err != nil || !found || len(updated.Candles) != 3 {
		t.Fatalf("new contract merge: candles=%d found=%t err=%v", len(updated.Candles), found, err)
	}
}

// TestFuturesSeriesSurviveRegistryTargetSwitch proves the restart contract:
// after a registry roll (old UID no longer targeted by the logical alias) the
// previously persisted old-contract series still loads by its physical UID.
func TestFuturesSeriesSurviveRegistryTargetSwitch(t *testing.T) {
	path := t.TempDir() + "/roll.sqlite"
	december := futuresKey("synthetic-uid-gold-2612")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(december, Snapshot{Candles: []model.Candle{testCandle(0, true)}, Metadata: Metadata{SourceFetchedAtUTCMS: 100, Coverage: []Interval{{FromUTCMS: 0, ToUTCMS: 60}}}}); err != nil {
		t.Fatal(err)
	}
	store.Close()
	// "Restart": reopen the same database; the series is keyed by the physical
	// UID, so it loads regardless of which contract a logical alias points at.
	restarted, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	got, found, err := restarted.Get(december, 0, 60)
	if err != nil || !found || len(got.Candles) != 1 {
		t.Fatalf("old contract series after restart: found=%t err=%v", found, err)
	}
}
