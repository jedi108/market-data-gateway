package storage

import (
	"testing"

	"github.com/jedi108/market-data-gateway/internal/model"
)

func TestLocalSaveMakesReplicaNonAuthoritative(t *testing.T) {
	store, err := Open(t.TempDir() + "/cache.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(testKey(), Snapshot{Candles: []model.Candle{testCandle(0, true)}, Metadata: Metadata{Replica: true, SourceFetchedAtUTCMS: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(testKey(), Snapshot{Candles: []model.Candle{testCandle(60, true)}, Metadata: Metadata{Replica: false, SourceFetchedAtUTCMS: 2}}); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Get(testKey(), 0, 120)
	if err != nil || !found || got.Metadata.Replica {
		t.Fatalf("local save did not make snapshot authoritative: %+v found=%t err=%v", got, found, err)
	}
}
