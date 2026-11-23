package crypto

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
)

// ReconciledMarkSource requires a pinned archive plus an independent API view.
type ReconciledMarkSource struct {
	client      *http.Client
	archiveBase string
	api         *BinanceMarkSource
	pin         func(string, string, string, string) (string, bool)
}

func NewReconciledMarkSource(client *http.Client) *ReconciledMarkSource {
	return &ReconciledMarkSource{client: client, archiveBase: "https://data.binance.vision", api: NewBinanceMarkSource(client), pin: markArchivePin}
}
func (s *ReconciledMarkSource) Fetch(ctx context.Context, key model.SeriesKey, from, to int64, limit int) ([]model.Candle, error) {
	fail := func() ([]model.Candle, error) {
		return nil, apperror.New(apperror.CodeCoverageUnknown, "mark source window unavailable", 0)
	}
	if s == nil || s.client == nil || s.api == nil || s.pin == nil || key.Validate() != nil || key.CandleType != "mark" || key.Timeframe != "1h" || from < 0 || to <= from || from%3600000 != 0 || to%3600000 != 0 || limit < 1 || limit > 1000 || (to-from)/3600000 > int64(limit) {
		return fail()
	}
	month := time.UnixMilli(from).UTC().Format("2006-01")
	if time.UnixMilli(to-1).UTC().Format("2006-01") != month {
		return fail()
	}
	pin, ok := s.pin(key.Venue, key.ProviderInstrumentID, key.Timeframe, month)
	if !ok {
		return fail()
	}
	if err := ratelimit.RequireScheduledAttempt(ctx); err != nil {
		return nil, err
	}
	path := "/data/futures/um/monthly/markPriceKlines/" + url.PathEscape(key.ProviderInstrumentID) + "/1h/" + url.PathEscape(key.ProviderInstrumentID+"-1h-"+month+".zip")
	fetch := func(address string, max int64) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
		if err != nil {
			return nil, err
		}
		client := *s.client
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("archive status %d", resp.StatusCode)
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
		if err != nil || int64(len(data)) > max {
			return nil, fmt.Errorf("archive body invalid")
		}
		return data, nil
	}
	data, err := fetch(s.archiveBase+path, 2<<20)
	if err != nil {
		return fail()
	}
	if err := ratelimit.ChargeAdditionalAttempt(ctx); err != nil {
		return nil, err
	}
	checksum, err := fetch(s.archiveBase+path+".CHECKSUM", 4096)
	if err != nil {
		return fail()
	}
	all, err := parseMarkArchive(data, string(checksum), pin, key.ProviderInstrumentID, key.Timeframe, month)
	if err != nil {
		return fail()
	}
	var archive []model.Candle
	for _, c := range all {
		if c.OpenTimeUTCMS >= from && c.CloseTimeUTCMS <= to {
			archive = append(archive, c)
		}
	}
	if int64(len(archive)) != (to-from)/3600000 {
		return fail()
	}
	for i, c := range archive {
		if c.OpenTimeUTCMS != from+int64(i)*3600000 {
			return fail()
		}
	}
	if err := ratelimit.ChargeAdditionalAttempt(ctx); err != nil {
		return nil, err
	}
	api, err := s.api.Fetch(ctx, key, from, to, limit)
	if err != nil {
		return nil, err
	}
	if int64(len(api)) != (to-from)/3600000 {
		return fail()
	}
	return ReconcileMark(archive, api, from, to, true)
}
