package apperror

import "testing"

func TestProviderRateLimitedIsRetryable(t *testing.T) {
	err := New(CodeProviderRateLimited, "budget exhausted", 1000)
	if !err.Retryable || err.HTTPStatus() != 429 || err.RetryAfterMS != 1000 {
		t.Fatalf("unexpected rate-limit error: %#v", err)
	}
}

func TestIncompleteCoverageIsTypedUnavailable(t *testing.T) {
	err := New(CodeIncompleteCoverage, "partial candle window", 0)
	if err.Retryable || err.HTTPStatus() != 503 || err.Code != CodeIncompleteCoverage {
		t.Fatalf("unexpected incomplete-coverage error: %#v", err)
	}
}

func TestUnknownSymbolIsNotRetryable(t *testing.T) {
	err := New(CodeUnknownSymbol, "unknown symbol", 0)
	if err.Retryable || err.HTTPStatus() != 422 {
		t.Fatalf("unexpected symbol error: %#v", err)
	}
}
