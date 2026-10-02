# Market Data Gateway

A Go market-data boundary service: a single caching, rate-limited access layer
between trading/runtime clients and upstream candle providers. It exposes
read-only candle APIs, makes no execution/order calls, and performs no provider
call during tests or startup validation.

## Features

- **Client API**: `/v1/health`, `/v1/ready`, `/v1/candles`, `/v1/candles/batch`, Prometheus `/v1/metrics`.
- **Fail-closed configuration**: strict YAML schema; the gateway refuses to start on unknown fields, missing values, or unsafe invariants.
- **Bounded upstream budget**: per-provider safe/node budget guard with runtime enforcement before every upstream call.
- **Cache + singleflight**: in-memory request coalescing and a persistent SQLite (pure-Go driver, CGO-free) replica cache with bounded retention.
- **Deterministic cluster routing** (optional multi-node): weighted rendezvous ownership per canonical series, private peer API with node-to-node verification, quorum/fencing lease for automatic failover, cluster-wide budget guard.
- **Providers**: TBank (generated gRPC bindings) and a deterministic fake provider for local development.

## Requirements

- Go 1.25+ (see `go.mod`), `CGO_ENABLED=0` supported and used in production builds.

## Build and test

```bash
make build          # CGO_ENABLED=0 binary in build/gateway
make test           # CGO_ENABLED=0 go test ./...
make test-race      # CGO_ENABLED=1 go test -race ./...
make vet
make fmt-check
make verify         # fmt-check + vet + test + local smoke
```

Generated TBank bindings are based on the pinned `RussianInvestments/invest-python` source revision `2a0074a` (Apache-2.0; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)); only `common.proto`, `marketdata.proto`, and `field_behavior.proto` are checked in under `internal/provider/tbank/gen`. The bindings are generated with `protoc v5.29.3`, `protoc-gen-go v1.36.12`, and `protoc-gen-go-grpc v1.5.1`; provider protobuf imports are deliberately confined to `internal/provider/tbank`.

## Quick start (fake provider, single node, loopback only)

No token, no VPN, no root, no non-loopback interface required:

```bash
make smoke
```

This builds the binary, starts it with `-provider=fake` on `127.0.0.1:8088`
using the committed local config [config/gateway.local.example.yaml](config/gateway.local.example.yaml),
performs a synthetic candles request, checks the metrics endpoint, stops the
process gracefully, and removes the temporary state file.

Manual equivalent:

```bash
make build
export GATEWAY_NODE_TOKEN=<any-non-empty-local-value>
./build/gateway -config config/gateway.local.example.yaml -provider=fake
curl -s http://127.0.0.1:8088/v1/health
curl -s 'http://127.0.0.1:8088/v1/candles?venue=tbank&symbol=SBER&timeframe=1m&from_utc_ms=0&to_utc_ms=180000&limit=500&include_incomplete=false'
```

The real TBank provider requires `TBANK_MARKET_DATA_TOKEN` (or the configured
`token_env`) plus an explicit `-tbank-endpoint`; it is never constructed
implicitly. Secrets are read from the environment only and never appear in
config files or logs.

## Configuration

`config/gateway.local.example.yaml` is a fully concrete single-node loopback
config for local development. `config/gateway.example.yaml` is a
non-deployable schema reference: every `REQUIRED_*` placeholder is fail-closed
and the gateway refuses to start without reviewed concrete values.

Peer-boundary invariant: multi-node membership, automatic failover, and
witnesses require private non-loopback peer addresses (fail-closed). The only
loopback exception is the single-node manual-mode local scenario above.

## Cluster mode

- `internal/routing`: weighted rendezvous ownership over canonical SeriesKey, salted by routing_version; deterministic primary/standby on all nodes; pinned golden vectors in `table_test.go`.
- `internal/auth`: node-credential domain for the peer boundary (constant-time, fail-closed when unconfigured).
- `internal/cluster`: membership view + one-hop peer client (typed `ROUTING_VERSION_MISMATCH`/`NOT_OWNER`/auth errors, bounded deadlines, response-owner verification) and the same-owner batch call `FetchCandlesBatch` (per-item typed errors).
- `internal/peerapi`: private peer handler (`/internal/v1/candles`, `/internal/v1/candles/batch`, `/internal/v1/node`); auth → cluster → hop → version → owner enforcement (per item for batches); never proxies (loops impossible).
- `internal/budget`: per-credential-group cluster budget guard. Every node derives it from the identical canonical config; `sum(node_hard_budgets_per_minute) <= safe_budget_per_minute` is enforced at config load (unsafe config refuses to start) and again at runtime before every upstream call. A provider-observed budget shrink below the configured sum latches upstream work closed until restart. Configured/observed/blocked state is exported as `gateway_cluster_budget_*` metrics.
- Batch: `POST /v1/candles/batch` partitions items by deterministic owner, runs local items through the normal singleflight `Get`, and sends one bounded peer sub-batch per remote owner (`batch_fan_out` bounds parallelism). Per-item result/error/freshness is preserved; peer responses are committed as read-only replicas exactly like the single-request path; a failing owner group only marks its own items.
- Failover: fencing-token lease per series key plus quorum confirmation; at most one upstream owner per series under crash, partition, or lease-store degradation — see [DOC/market_data_gateway_failover.md](DOC/market_data_gateway_failover.md).

Cluster smoke targets (multi-node smokes bind peers to a discovered private
IPv4 and clients to loopback): `make smoke-cluster2`, `make smoke-cluster3`,
`make smoke-loglevel`, `make smoke-packaging`.

## Storage and schema migrations

The SQLite replica cache schema is owned by this repository:
`internal/storage/migrations` holds the ordered, forward-only migration chain
and a single `CurrentVersion`. On open, the gateway applies pending migrations
transactionally, safely completes legacy databases, and refuses (fail-closed)
any on-disk schema newer than the binary supports. There is no operational
config knob for the schema version — pinning a gateway version pins its schema.

## Capacity planning

The optional `capacity_plan` switch is documented in [DOC/capacity_plan.md](DOC/capacity_plan.md).
`off` preserves runtime behavior, `shadow` forecasts without side effects, and
`enforce` fail-closes startup/readiness for an invalid or infeasible reviewed
plan. The existing scheduler remains the real-time admission authority.

## Design docs

- [DOC/market_data_gateway_failover.md](DOC/market_data_gateway_failover.md) — automatic failover contract and fault tests.
- [DOC/capacity_plan.md](DOC/capacity_plan.md) — capacity-plan switch semantics.
- [DOC/adr/ADR-007-capacity-planning.md](DOC/adr/ADR-007-capacity-planning.md) — credential-group budget model.

## Futures support and capability boundary (F20)

The gateway serves **candles only** from the TBank market-data surface
(`MarketDataService.GetCandles`; its `instrument_id` accepts both a share FIGI
and a physical futures contract `instrument_uid`, so futures need no order or
trading capability). Everything else a TBank account can do — orderbooks,
trade/quote streams, last prices, instrument metadata lookups, and any order
or sandbox RPC — stays with **direct TBank consumers** and is never proxied
through the gateway. Silent fallback between sources is forbidden: an input
outside the boundary fails with the typed `UNSUPPORTED_CAPABILITY` error
(`internal/provider/tbank/capabilities.go`), never an empty response; an
unregistered symbol fails with `UNKNOWN_SYMBOL` before any provider call.

Futures identity contract:

- Series identity is the **physical provider instrument UID** (per expiry),
  never the logical alias: `model.SeriesKey` keys cache/SQLite by
  venue|market|UID|timeframe|candle, so different expiries are different series
  and a contract's history is never mixed with or rewritten by its successor.
  The storage schema version stays owned by the migration chain; the response
  envelope carries its own `schema_version`.
- A logical alias (for example a rolling `GOLD` ticker) is a **resolution
  input only**: within one registry snapshot it targets exactly one physical
  contract, and an alias declared on two contracts is a fail-closed
  ambiguous-alias configuration error.
- Responses carry explicit `instrument` metadata (`instrument_type`,
  canonical physical symbol, provider-id kind, `price_unit` — `points` for
  futures / `currency` for shares, `volume_unit` — `lots`, quote currency,
  and the futures `expiration_utc_ms`) so downstream never reconstructs the
  physical contract or its units from a ticker. Share responses gain the same
  additive field with their series identity unchanged.

Registry refresh and restart recovery (`-tbank-instruments-file`): the
instrument mapping can be replaced atomically (validate the full candidate
snapshot, persist it with a temp-file rename, then swap the in-memory mapping
under lock); on restart the persisted file is recovered (missing file falls
back to the compiled-in reviewed list; corrupt or foreign-schema files fail
closed).

With the same file configured plus a positive
`instrument_registry.refresh_interval_ms`, a background refresher re-reads the
file on that cadence and applies changed snapshots at runtime — no restart
needed for a futures roll. A corrupt or invalid candidate is rejected with a
warning and the currently served mapping stays untouched; a missing file is a
no-op (the file never silently rolls the mapping back); every applied refresh
is durably persisted (normalized temp-file rename), increments
`gateway_registry_refreshes_total`, and re-records the registry gauge set.
Without a registry file the compiled-in list is served and refresh is refused.
The compiled-in production list carries the reviewed physical GOLD-12.26
contract (verified against live TBank by the read-only F20 online DoD: candles
via `instrument_id` UID, prices in points, integer lot volume); synthetic
futures instruments exist only in tests (guard-pinned).

F20 metrics: `gateway_registry_instruments` and
`gateway_registry_active_contract_expiry_epoch_seconds` (active physical
contracts), `gateway_registry_refreshes_total` /
`gateway_registry_schema_version` (registry updates and schema),
`gateway_series_cache_source_total` (cache source by market type), and
`gateway_series_source_age_seconds` (fetch-provenance age, separate from the
market-age gauge).

## Project status

Infrastructure code extracted for portfolio use; API and configuration may
change. This is not a finished financial product and carries no SLA. There is
no execution/order functionality by design.

## License

The source in this repository is publicly readable but is **not** provided
under an open-source license: no rights to reuse, modify, or redistribute are
granted automatically. Third-party generated protocol bindings carry their own
attribution — see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
