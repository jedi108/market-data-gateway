package httpapi

import (
	"encoding/json"
	"net/http"
)

type HealthHandler struct {
	build  string
	nodeID string // optional; omitted from answers when empty
	ready  func() bool
}

func NewHealthHandler(build string, ready func() bool) *HealthHandler {
	return &HealthHandler{build: build, ready: ready}
}
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/v1/health":
		write(w, http.StatusOK, h.body("ok"))
	case "/v1/ready":
		if h.ready() {
			write(w, http.StatusOK, h.body("ready"))
		} else {
			write(w, http.StatusServiceUnavailable, h.body("not_ready"))
		}
	default:
		http.NotFound(w, r)
	}
}
func (h *HealthHandler) body(status string) map[string]any {
	body := map[string]any{"status": status, "build": h.build}
	if h.nodeID != "" {
		body["node_id"] = h.nodeID
	}
	return body
}
func write(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
