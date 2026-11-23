package crypto

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
)

// The archive and its published CHECKSUM were independently downloaded and
// compared on 2026-11-23 from:
// https://data.binance.vision/data/futures/um/monthly/markPriceKlines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip
// The companion checksum URL appends .CHECKSUM.
func markArchivePin(venue, symbol, timeframe, month string) (string, bool) {
	if venue == "binance" && symbol == "BTCUSDT" && timeframe == "1h" && month == "2024-01" {
		return "759f86a22dadb455c87a3f90f6a9134c73d24a55a7596f0b96e193b5d45cdb49", true
	}
	return "", false
}

// parseMarkArchive checks the independent pin, upstream checksum and entire
// monthly archive before exposing any candle. Currently only 1h is pinned.
func parseMarkArchive(data []byte, checksum, pin, symbol, timeframe, month string) ([]model.Candle, error) {
	fail := func() ([]model.Candle, error) {
		return nil, apperror.New(apperror.CodeCoverageUnknown, "mark archive invalid", 0)
	}
	if symbol == "" || strings.Trim(symbol, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") != "" || timeframe != "1h" {
		return fail()
	}
	m, err := time.Parse("2006-01", month)
	if err != nil || m.Format("2006-01") != month {
		return fail()
	}
	name := symbol + "-" + timeframe + "-" + month + ".zip"
	hash := sha256.Sum256(data)
	actual := hex.EncodeToString(hash[:])
	fields := strings.Fields(checksum)
	if len(fields) != 2 || fields[0] != actual || fields[1] != name || pin != actual {
		return fail()
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil || len(zr.File) != 1 {
		return fail()
	}
	f := zr.File[0]
	if f.Name != strings.TrimSuffix(name, ".zip")+".csv" || f.UncompressedSize64 > 1<<20 {
		return fail()
	}
	r, err := f.Open()
	if err != nil {
		return fail()
	}
	defer r.Close()
	contents, err := io.ReadAll(io.LimitReader(r, (1<<20)+1))
	if err != nil || len(contents) > 1<<20 {
		return fail()
	}
	csvr := csv.NewReader(bytes.NewReader(contents))
	header, err := csvr.Read()
	if err != nil || strings.Join(header, ",") != "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore" {
		return fail()
	}
	var result []model.Candle
	previous := int64(-1)
	for {
		row, err := csvr.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(row) != 12 {
			return fail()
		}
		open, e1 := strconv.ParseInt(row[0], 10, 64)
		closeMS, e2 := strconv.ParseInt(row[6], 10, 64)
		if e1 != nil || e2 != nil || open < m.UnixMilli() || open >= m.AddDate(0, 1, 0).UnixMilli() || open <= previous || open%3600000 != 0 || closeMS != open+3600000-1 {
			return fail()
		}
		c := model.Candle{OpenTimeUTCMS: open, CloseTimeUTCMS: open + 3600000, Open: row[1], High: row[2], Low: row[3], Close: row[4], Volume: row[5], IsClosed: true}
		if c.Validate() != nil {
			return fail()
		}
		result = append(result, c)
		previous = open
	}
	if len(result) == 0 {
		return fail()
	}
	return result, nil
}
