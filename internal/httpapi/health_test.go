package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthAndReady(t *testing.T) {
	h := NewHealthHandler("test-build", func() bool { return true })
	for _, path := range []string{"/v1/health", "/v1/ready"} {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
		if r.Code != http.StatusOK {
			t.Fatalf("%s status = %d", path, r.Code)
		}
	}
}

func TestReadyReportsUnavailable(t *testing.T) {
	h := NewHealthHandler("test-build", func() bool { return false })
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/v1/ready", nil))
	if r.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", r.Code)
	}
}
