# Contributing

## Development

Requirements: Go 1.25+ (see `go.mod`), `bash`, `curl`.

```bash
make build       # CGO_ENABLED=0 binary in build/gateway
make test        # CGO_ENABLED=0 go test ./...
make test-race   # CGO_ENABLED=1 go test -race ./...
make vet
make fmt-check
make smoke       # local fake-provider end-to-end smoke
make verify      # fmt-check + vet + test + smoke
```

## Style rules

- Run `gofmt` on all Go code; CI rejects unformatted files.
- Keep configuration handling fail-closed: new config fields must have explicit
  validation, and unknown fields must be rejected.
- Never log or persist credentials; secrets come from the environment only.
- New storage schema changes must be added as an ordered, forward-only
  migration in `internal/storage/migrations` with lifecycle test coverage;
  never edit an already-shipped migration.
- Multi-node / automatic-failover behavior must keep requiring private
  non-loopback peer addresses; do not widen the single-node loopback exception.
- Tests must not require network access beyond loopback or any external
  account; use the fake provider.

## Pull requests

- Keep PRs focused; include tests for behavior changes.
- CI must pass: formatting, vet, unit tests, race detector, and the local
  smoke job.
