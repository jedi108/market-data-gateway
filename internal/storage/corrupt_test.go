package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/storage/migrations"
)

// TestOpenFailsClosedOnCorruptDatabase proves the Gate 1 corrupt-cache
// contract: a damaged SQLite file is never silently recreated or accepted;
// startup fails closed instead of serving unknown state.
func TestOpenFailsClosedOnCorruptDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.sqlite")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	// Corrupt the header AND middle pages of what was a valid database.
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("GARBAGEGARBAGE!"), 0); err != nil {
		t.Fatal(err)
	}
	if info.Size() > 4096 {
		if _, err := file.WriteAt([]byte("GARBAGEGARBAGE!"), info.Size()/2); err != nil {
			t.Fatal(err)
		}
	}
	file.Close()

	if _, err := Open(path); err == nil {
		t.Fatal("Open accepted a corrupt database; contract requires fail-closed rejection")
	}
}

// TestOpenRejectsNewerSchemaVersion is the destructive-migration guard: an
// on-disk schema newer than the binary must never be migrated downward. The
// newer database is produced only through the test-only registry; production
// Open (CurrentVersion = 1) must reject it fail-closed.
func TestOpenRejectsNewerSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema.sqlite")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	chain, err := migrations.BuildTestRegistry(migrations.Migration{
		Version: 2,
		Name:    "v2_test_probe_table",
		Up: func(exec migrations.Executor) error {
			_, err := exec.Exec("CREATE TABLE IF NOT EXISTS v2_probe (id INTEGER PRIMARY KEY)")
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	upgraded, err := openWithRegistry(path, chain)
	if err != nil {
		t.Fatal(err)
	}
	upgraded.Close()

	if _, err := Open(path); err == nil {
		t.Fatal("Open accepted an on-disk schema newer than the binary supports")
	}
}
