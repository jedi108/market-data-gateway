package crypto

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestParseMarkArchivePinAndSchema(t *testing.T) {
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	w, err := z.Create("BTCUSDT-1h-2024-01.csv")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(w, "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1704067200000,10,12,9,11,0,1704070799999,0,3600,0,0,0\n")
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(b.Bytes()))
	checksum := hash + "  BTCUSDT-1h-2024-01.zip"
	rows, err := parseMarkArchive(b.Bytes(), checksum, hash, "BTCUSDT", "1h", "2024-01")
	if err != nil || len(rows) != 1 || rows[0].CloseTimeUTCMS != 1704070800000 {
		t.Fatalf("rows=%v error=%v", rows, err)
	}
	for _, pin := range []string{"", "bad"} {
		if _, err := parseMarkArchive(b.Bytes(), checksum, pin, "BTCUSDT", "1h", "2024-01"); err == nil {
			t.Fatal("untrusted pin accepted")
		}
	}
	if _, err := parseMarkArchive(b.Bytes(), checksum+"x", hash, "BTCUSDT", "1h", "2024-01"); err == nil {
		t.Fatal("checksum name accepted")
	}
	if got, ok := markArchivePin("binance", "BTCUSDT", "1h", "2024-01"); !ok || got != "759f86a22dadb455c87a3f90f6a9134c73d24a55a7596f0b96e193b5d45cdb49" {
		t.Fatal("verified pin missing")
	}
	if _, ok := markArchivePin("binance", "BTCUSDT", "1h", "2024-02"); ok {
		t.Fatal("unpinned month accepted")
	}
}
