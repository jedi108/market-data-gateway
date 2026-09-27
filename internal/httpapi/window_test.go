package httpapi

import (
	"net/http"
	"testing"

	"github.com/jedi108/market-data-gateway/internal/apperror"
)

func TestValidateWindowOneHourAcceptsBoundedAlignedRange(t *testing.T) {
	if err := validateWindow("1h", 0, 9*60*60*1000, 10); err != nil {
		t.Fatalf("aligned bounded window rejected: %v", err)
	}
}

func TestValidateWindowOneHourRejectsUnalignedRange(t *testing.T) {
	err := validateWindow("1h", 1, 60*60*1000, 1)
	if typed, ok := err.(*apperror.Error); !ok || typed.Code != apperror.CodeInvalidRequest || typed.HTTPStatus() != http.StatusBadRequest {
		t.Fatalf("unexpected alignment error: %T %v", err, err)
	}
}

func TestValidateWindowOneHourRejectsMoreCandlesThanLimit(t *testing.T) {
	err := validateWindow("1h", 0, 2*60*60*1000, 1)
	if typed, ok := err.(*apperror.Error); !ok || typed.Code != apperror.CodeInvalidRequest {
		t.Fatalf("unexpected bound error: %T %v", err, err)
	}
}
