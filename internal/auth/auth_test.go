package auth

import (
	"net/http"
	"testing"
)

func requestWithToken(token string) *http.Request {
	r, _ := http.NewRequest(http.MethodPost, "/internal/v1/candles", nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

func TestAuthenticateAcceptsValidToken(t *testing.T) {
	a := NewNodeAuth("secret-node-token")
	if err := a.Authenticate(requestWithToken("secret-node-token")); err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
	if !a.Enabled() {
		t.Fatal("expected enabled")
	}
}

func TestAuthenticateRejectsWrongToken(t *testing.T) {
	a := NewNodeAuth("secret-node-token")
	if err := a.Authenticate(requestWithToken("wrong")); err == nil {
		t.Fatal("wrong token accepted")
	}
}

func TestAuthenticateRejectsMissingHeader(t *testing.T) {
	a := NewNodeAuth("secret-node-token")
	if err := a.Authenticate(requestWithToken("")); err == nil {
		t.Fatal("missing header accepted")
	}
}

func TestEmptyTokenFailsClosed(t *testing.T) {
	a := NewNodeAuth("")
	if a.Enabled() {
		t.Fatal("empty token must disable auth")
	}
	// Even the empty presentation must be rejected — never an open boundary.
	if err := a.Authenticate(requestWithToken("")); err == nil {
		t.Fatal("disabled auth accepted a request")
	}
	if err := a.Authenticate(requestWithToken("anything")); err == nil {
		t.Fatal("disabled auth accepted a request")
	}
}

func TestAuthenticateRejectsWrongScheme(t *testing.T) {
	a := NewNodeAuth("secret-node-token")
	r, _ := http.NewRequest(http.MethodPost, "/internal/v1/candles", nil)
	r.Header.Set("Authorization", "Basic secret-node-token")
	if err := a.Authenticate(r); err == nil {
		t.Fatal("wrong scheme accepted")
	}
}
