package crypto

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/jedi108/market-data-gateway/internal/apperror"
	"strings"
	"testing"
)

func fundingFixture(t *testing.T, member, body string) ([]byte, string, string) {
	t.Helper()
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	f, err := w.Create(member)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b.Bytes())
	sha := hex.EncodeToString(h[:])
	return b.Bytes(), sha + "  BTCUSDT-fundingRate-2024-01.zip\n", sha
}
func TestParseFundingArchive(t *testing.T) {
	const member = "BTCUSDT-fundingRate-2024-01.csv"
	const header = "calc_time,funding_interval_hours,last_funding_rate\n"
	data, checksum, sha := fundingFixture(t, member, header+"1704067200000,8,0.00037409\n1704096000000,4,-0.0001\n")
	events, err := ParseFundingArchive(data, checksum, sha, "BTCUSDT", "2024-01")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].IntervalH != 8 || events[1].IntervalH != 4 || events[1].AvailableAt != events[1].Ts || events[1].Availability != "historical_payment_assumption" {
		t.Fatalf("unexpected events: %+v", events)
	}
	for _, tt := range []struct{ name, member, body string }{
		{"schema", member, "wrong,header,columns\n"}, {"header only", member, header}, {"duplicate", member, header + "1704067200000,8,0.1\n1704067200000,4,0.2\n"},
		{"order", member, header + "1704096000000,8,0.1\n1704067200000,4,0.2\n"}, {"other month", member, header + "1706745600000,8,0.1\n"},
		{"wrong symbol", "ETHUSDT-fundingRate-2024-01.csv", header}, {"invalid interval", member, header + "1704067200000,0,0.1\n"}, {"invalid rate", member, header + "1704067200000,8,nan\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, c, s := fundingFixture(t, tt.member, tt.body)
			if _, err := ParseFundingArchive(d, c, s, "BTCUSDT", "2024-01"); err != nil {
				var typed *apperror.Error
				if !errors.As(err, &typed) || typed.Code != apperror.CodeCoverageUnknown {
					t.Fatalf("expected typed coverage error, got %v", err)
				}
			} else {
				t.Fatal("expected refusal")
			}
		})
	}
	t.Run("member CRC", func(t *testing.T) {
		corrupt := append([]byte(nil), data...)
		central := bytes.Index(corrupt, []byte{'P', 'K', 1, 2})
		if central < 0 {
			t.Fatal("central directory not found")
		}
		corrupt[central+16] ^= 0xff
		hash := sha256.Sum256(corrupt)
		pin := hex.EncodeToString(hash[:])
		if _, err := ParseFundingArchive(corrupt, pin+"  BTCUSDT-fundingRate-2024-01.zip", pin, "BTCUSDT", "2024-01"); err == nil {
			t.Fatal("expected CRC refusal")
		}
	})
	t.Run("oversized member", func(t *testing.T) {
		d, c, s := fundingFixture(t, member, header+strings.Repeat(" ", 1<<20))
		if _, err := ParseFundingArchive(d, c, s, "BTCUSDT", "2024-01"); err == nil {
			t.Fatal("expected size refusal")
		}
	})
	if _, err = ParseFundingArchive(data, checksum, "bad", "BTCUSDT", "2024-01"); err == nil {
		t.Fatal("expected pin refusal")
	}
	if _, err = ParseFundingArchive(data, "bad", sha, "BTCUSDT", "2024-01"); err == nil {
		t.Fatal("expected checksum refusal")
	}
}
