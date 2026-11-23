package crypto

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
)

// BinanceMarkSource reads USD-M mark-price klines inside the scheduler.
type BinanceMarkSource struct {
	client  *http.Client
	apiBase string
	now     func() time.Time
}

func NewBinanceMarkSource(client *http.Client) *BinanceMarkSource {
	return &BinanceMarkSource{client: client, apiBase: "https://fapi.binance.com", now: time.Now}
}

func (s *BinanceMarkSource) Fetch(ctx context.Context, key model.SeriesKey, fromMS, toMS int64, limit int) ([]model.Candle, error) {
	fail := func() ([]model.Candle, error) {
		return nil, apperror.New(apperror.CodeCoverageUnknown, "mark source window unavailable", 0)
	}
	if s == nil || s.client == nil || s.now == nil || key.Validate() != nil || key.CandleType != "mark" || fromMS < 0 || toMS <= fromMS || limit < 1 || limit > 1000 {
		return fail()
	}
	// Only fixed-duration Binance intervals are supported; calendar months need different alignment.
	durations := map[string]int64{"1m": 60000, "3m": 180000, "5m": 300000, "15m": 900000, "30m": 1800000, "1h": 3600000, "2h": 7200000, "4h": 14400000, "6h": 21600000, "8h": 28800000, "12h": 43200000, "1d": 86400000, "3d": 259200000, "1w": 604800000}
	step, ok := durations[key.Timeframe]
	if !ok {
		return fail()
	}
	if err := ratelimit.RequireScheduledAttempt(ctx); err != nil {
		return nil, err
	}
	q := url.Values{"symbol": {key.ProviderInstrumentID}, "interval": {key.Timeframe}, "startTime": {strconv.FormatInt(fromMS, 10)}, "endTime": {strconv.FormatInt(toMS-1, 10)}, "limit": {strconv.Itoa(limit)}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.apiBase+"/fapi/v1/markPriceKlines?"+q.Encode(), nil)
	if err != nil {
		return fail()
	}
	client := *s.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return fail()
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fail()
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return fail()
	}
	var rows [][]json.RawMessage
	if json.Unmarshal(body, &rows) != nil || len(rows) == 0 || len(rows) >= limit {
		return fail()
	}
	result := make([]model.Candle, 0, len(rows))
	previous := int64(-1)
	now := s.now().UnixMilli()
	for _, row := range rows {
		if len(row) < 7 {
			return fail()
		}
		var open, closeMS int64
		var o, h, l, c string
		if json.Unmarshal(row[0], &open) != nil || json.Unmarshal(row[1], &o) != nil || json.Unmarshal(row[2], &h) != nil || json.Unmarshal(row[3], &l) != nil || json.Unmarshal(row[4], &c) != nil || json.Unmarshal(row[6], &closeMS) != nil || open < fromMS || open >= toMS || open <= previous || open > (1<<63-1)-step || closeMS != open+step-1 {
			return fail()
		}
		previous = open
		candle := model.Candle{OpenTimeUTCMS: open, CloseTimeUTCMS: open + step, Open: o, High: h, Low: l, Close: c, Volume: "0", IsClosed: open+step <= now}
		if err := candle.Validate(); err != nil {
			return fail()
		}
		if candle.IsClosed && candle.CloseTimeUTCMS <= toMS {
			result = append(result, candle)
		}
	}
	if len(result) == 0 {
		return fail()
	}
	return result, nil
}
