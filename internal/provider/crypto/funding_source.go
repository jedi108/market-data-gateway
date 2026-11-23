package crypto

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
)

// BinanceFundingSource reads the pinned archive and compares it with the API.
// Fetch must run inside the existing scheduler; each HTTP attempt is charged.
type BinanceFundingSource struct {
	client               *http.Client
	archiveBase, apiBase string
	pin                  func(string, string, string) (string, bool)
}

func NewBinanceFundingSource(client *http.Client) *BinanceFundingSource {
	return &BinanceFundingSource{client: client, archiveBase: "https://data.binance.vision", apiBase: "https://fapi.binance.com", pin: fundingArchivePin}
}

func (s *BinanceFundingSource) Fetch(ctx context.Context, symbol string, fromMS, toMS int64) ([]model.FundingEvent, error) {
	unknown := func() ([]model.FundingEvent, error) {
		return nil, apperror.New(apperror.CodeCoverageUnknown, "funding source window unavailable or unreconciled", 0)
	}
	if s == nil || s.client == nil || s.pin == nil || fromMS < 0 || toMS <= fromMS {
		return unknown()
	}
	start := time.UnixMilli(fromMS).UTC()
	end := time.UnixMilli(toMS - 1).UTC()
	if start.Year() != end.Year() || start.Month() != end.Month() {
		return unknown()
	}
	month := start.Format("2006-01")
	pin, ok := s.pin("binance", symbol, month)
	if !ok {
		return unknown()
	}
	if err := ratelimit.RequireScheduledAttempt(ctx); err != nil {
		return nil, err
	}
	archivePath := "/data/futures/um/monthly/fundingRate/" + url.PathEscape(symbol) + "/" + url.PathEscape(symbol+"-fundingRate-"+month+".zip")
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
			return nil, fmt.Errorf("upstream status %d", resp.StatusCode)
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
		if err != nil || int64(len(data)) > max {
			return nil, fmt.Errorf("upstream body invalid or oversized")
		}
		return data, nil
	}
	data, err := fetch(s.archiveBase+archivePath, 2<<20)
	if err != nil {
		return unknown()
	}
	if err = ratelimit.ChargeAdditionalAttempt(ctx); err != nil {
		return nil, err
	}
	checksum, err := fetch(s.archiveBase+archivePath+".CHECKSUM", 4096)
	if err != nil {
		return unknown()
	}
	events, err := ParseFundingArchive(data, string(checksum), pin, symbol, month)
	if err != nil {
		return unknown()
	}
	var window []model.FundingEvent
	for _, event := range events {
		if event.Ts >= fromMS && event.Ts < toMS {
			window = append(window, event)
		}
	}
	if err = ratelimit.ChargeAdditionalAttempt(ctx); err != nil {
		return nil, err
	}
	q := url.Values{"symbol": {symbol}, "startTime": {strconv.FormatInt(fromMS, 10)}, "endTime": {strconv.FormatInt(toMS-1, 10)}, "limit": {"1000"}}
	body, err := fetch(s.apiBase+"/fapi/v1/fundingRate?"+q.Encode(), 1<<20)
	if err != nil {
		return unknown()
	}
	var raw []struct {
		Symbol      string `json:"symbol"`
		FundingTime int64  `json:"fundingTime"`
		FundingRate string `json:"fundingRate"`
	}
	if err = json.Unmarshal(body, &raw); err != nil || len(raw) >= 1000 {
		return unknown()
	}
	api := make([]FundingAPIRate, 0, len(raw))
	for _, v := range raw {
		if v.Symbol != symbol {
			return unknown()
		}
		api = append(api, FundingAPIRate{Ts: v.FundingTime, Rate: v.FundingRate})
	}
	return ReconcileFunding(window, api, fromMS, toMS, true)
}
