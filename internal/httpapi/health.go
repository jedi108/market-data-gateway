package httpapi

import (
	"encoding/json"
	"net/http"
)

type HealthHandler struct {
	build string
	ready func() bool
}

func NewHealthHandler(build string, ready func() bool) *HealthHandler {
	return &HealthHandler{build: build, ready: ready}
}
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/v1/health":
		write(w, http.StatusOK, map[string]any{"status": "ok", "build": h.build})
	case "/v1/ready":
		if h.ready() {
			write(w, http.StatusOK, map[string]any{"status": "ready", "build": h.build})
		} else {
			write(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "build": h.build})
		}
	default:
		http.NotFound(w, r)
	}
}
func write(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
