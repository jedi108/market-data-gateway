package crypto

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jedi108/market-data-gateway/internal/model"
)

var fundingRatePattern = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)

// ParseFundingArchive verifies one pinned Binance monthly funding archive.
// Verified rows confirm their own intervals only, never coverage of a range.
func ParseFundingArchive(data []byte, checksum, pinnedSHA, symbol, month string) ([]model.FundingEvent, error) {
	fail := func(reason string) ([]model.FundingEvent, error) {
		return nil, fmt.Errorf("funding_missing: %s", reason)
	}
	if symbol == "" || strings.Trim(symbol, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") != "" || !strings.HasSuffix(symbol, "USDT") {
		return fail("invalid Binance USD-M symbol")
	}
	m, err := time.Parse("2006-01", month)
	if err != nil || m.Format("2006-01") != month {
		return fail("invalid month")
	}
	name := symbol + "-fundingRate-" + month + ".zip"
	fields := strings.Fields(checksum)
	hash := sha256.Sum256(data)
	actual := hex.EncodeToString(hash[:])
	if len(fields) != 2 || fields[0] != actual || fields[1] != name || pinnedSHA != actual {
		return fail("archive checksum or pin mismatch")
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil || len(zr.File) != 1 {
		return fail("invalid archive")
	}
	file := zr.File[0]
	if file.Name != strings.TrimSuffix(name, ".zip")+".csv" || file.UncompressedSize64 > 1<<20 {
		return fail("invalid archive member")
	}
	r, err := file.Open()
	if err != nil {
		return fail("cannot open archive member")
	}
	defer r.Close()
	csvr := csv.NewReader(io.LimitReader(r, 1<<20+1))
	header, err := csvr.Read()
	if err != nil || strings.Join(header, ",") != "calc_time,funding_interval_hours,last_funding_rate" {
		return fail("invalid archive schema")
	}
	var events []model.FundingEvent
	var previous int64 = -1
	for {
		row, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(row) != 3 {
			return fail("invalid archive row")
		}
		ts, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil || ts < m.UnixMilli() || ts >= m.AddDate(0, 1, 0).UnixMilli() || ts <= previous {
			return fail("invalid timestamp order or month")
		}
		interval, err := strconv.Atoi(row[1])
		if err != nil || interval <= 0 {
			return fail("invalid funding interval")
		}
		if _, ok := new(big.Rat).SetString(row[2]); !ok || !fundingRatePattern.MatchString(row[2]) {
			return fail("invalid funding rate")
		}
		events = append(events, model.FundingEvent{Ts: ts, Rate: row[2], IntervalH: interval, AvailableAt: ts, Availability: "historical_payment_assumption"})
		previous = ts
	}
	return events, nil
}
