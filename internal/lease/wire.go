package lease

// AcquireRequest is the wire contract for POST /internal/v1/lease/acquire.
type AcquireRequest struct {
	Series  string `json:"series"`
	Holder  string `json:"holder"`
	Version int    `json:"version"`
	TTLMS   int64  `json:"ttl_ms"`
	Token   Token  `json:"token"`
}

// StateEnvelope is the wire contract for lease peek/acquire responses.
type StateEnvelope struct {
	Series    string `json:"series"`
	Owner     string `json:"owner"`
	Token     uint64 `json:"token"`
	Version   int    `json:"version"`
	ExpiresAt int64  `json:"expires_at"`
	IssuedAt  int64  `json:"issued_at"`
}

// ToState converts the wire envelope back into the internal State. Token is
// widened from uint64; the internal Token is uint64-backed and safe for all
// realistic fencing sequences.
func (e StateEnvelope) ToState() State {
	return State{
		Owner:     e.Owner,
		Token:     Token(e.Token),
		Version:   e.Version,
		ExpiresAt: e.ExpiresAt,
		IssuedAt:  e.IssuedAt,
	}
}
