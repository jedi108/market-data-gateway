# Security policy

## Supported versions

Only the latest commit on `main` is supported.

## Reporting a vulnerability

Open a private GitHub security advisory for this repository ("Report a
vulnerability" on the Security tab). Please do not open a public issue for
security problems.

Include a description, reproduction steps, and the affected component
(`internal/...` package or `cmd/gateway`) if known.

## Security model notes

- The gateway is intentionally read-only: it serves candle data and exposes no
  execution/order functionality.
- Configuration is strict and fail-closed: unknown fields, missing values, or
  unsafe invariants prevent startup.
- Provider and peer credentials are read from the environment only; they are
  never written to configuration files, logs, or metrics.
- The client HTTP listener must bind a loopback address; the peer listener is
  intended for private networks and requires explicit node-token
  authentication.
- SQLite schema migrations are owned by the binary and are applied
  transactionally; an on-disk schema newer than the binary refuses to start.
