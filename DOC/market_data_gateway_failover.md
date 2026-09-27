# Automatic Failover (market-data-gateway)

Status: implemented (design + fault tests + wired into the data plane).

This document is the source of truth for the multi-node automatic-failover
contract for the market-data-gateway. It covers the central safety invariant:

> **Ownership gate — at most one upstream-owner per `SeriesKey`**, under crash,
> network partition, and lease-store degradation.

---

## 1. Goals (and non-goals)

In scope:

- Fencing-token lease per `SeriesKey` so a standby node can take over upstream
  provider ownership from a dead primary without creating a second concurrent
  owner.
- Quorum confirmation of every takeover across the full configured membership
  (primary + standby + witness nodes), so a minority partition cannot win.
- Fail-closed behavior: a node that cannot prove ownership (lease missing,
  expired, or fenced) is **denied the provider call** by the data plane and
  falls back to the stale/replica path — it never becomes an un-fenced owner.
- A synchronous fallback path: if the lease store is degraded, the deterministic
  manual ownership decision still holds (no lease takeover), preserving the
  existing routing guarantee as the rollback path.

Out of scope (explicitly):

- Provider-side resilience (that is the existing peer fallback / budget guard).
- Live trading decision changes — this is a *data* gateway; ownership only
  controls who calls the upstream provider.
- Rebalancing strategy beyond the deterministic routing table.

---

## 2. Ownership model

Two layers, both eventually consistent with the same `SeriesKey`:

1. **Deterministic routing table** (`internal/routing`) — weighted rendezvous
   hashing over the canonical `SeriesKey` identity yields a primary and a
   standby per series. This is the **source of truth for who *may* own**. It is
   config-hash pinned (`routing_hash`) and identical on every node.
2. **Lease control plane** (`internal/lease`) — a per-`SeriesKey` fencing token
   that records *who currently holds the upstream provider right*. This is the
   **source of truth for who *does* own right now**, and it can only ever be
   granted to the deterministic primary or standby.

The gate (`lease.Gate`) is the enforcement point: a node may call the provider
for a series **only if** it holds a valid, unexpired lease whose token is not
behind the observed committed token.

```
            routing table (deterministic, pinned by routing_hash)
                          │  primary + standby per SeriesKey
                          ▼
                  lease.Coordinator.Takeover / Renew
                          │  quorum (strict majority of ALL members)
                          ▼
                  lease.NodeStore (held lease + observed truth)
                          │
                          ▼
                  lease.Gate.MayCallProvider  ──►  service.refresh
                          │
                  ALLOWED  →  provider call
                  DENIED   →  stale/replica fallback (never a second owner)
```

---

## 3. Lease semantics

A lease is `(owner, token, version, expires_at, issued_at)`.

- **Token** is a monotonically increasing `uint64` per `SeriesKey`. Fencing is
  forward-only: a candidate may only win with `token > observed.token`. Same or
  lower token is `LEASE_STALE`. This is the atomic unit of split-brain
  prevention.
- **Hold vs observed**: the local node only marks a lease *held* (provider right
  granted) once a quorum is won and the returned token is greater than the
  observed token. A provisionally committed token that lost the race never
  authorizes provider work.
- **Expiry**: a held lease is valid only while `now < expires_at`. The background
  renewer re-confirms each held lease on a strict-majority cadence at ~1/3 of the
  TTL. A renewal that loses quorum drops the held lease (fail-closed) so the gate
  stops the provider right.
- **Version** is the routing version. A takeover is only valid at the current
  `routing_version`; a version bump changes the deterministic primary/standby,
  which is the manual switch path and the rollback mechanism.

### Quorum basis

Quorum is computed over **total configured membership** (`local node + all
witnesses`), never over the merely-reachable subset. An isolated node with `N`
members but only itself reachable needs a strict majority of `N`, which it can
never reach. This is what makes a minority partition fail closed.

```
func quorumMajority(total) = total/2 + 1   // strict majority
```

A takeover wins when `self-vote + Peek-agree + committed-Acquire-votes >= majority`.
The local node counts as one vote (its `NodeStore`); each reachable witness that
agrees or commits counts one. Unreachable witnesses contribute **zero** — they are
never assumed to agree.

---

## 4. Data-plane enforcement

`service.refresh` consults `cfg.LeaseGate` (nil when failover mode is manual):

```go
if s.cfg.LeaseGate != nil {
    if d := s.cfg.LeaseGate.MayCallProvider(request.Series.Identity()); !d.Allowed {
        return &upstreamFailure{err: d.DeniedAsError()}
    }
}
```

The gate decision:

- **Manual mode** (no gate): the deterministic primary owns; no lease logic runs.
  This is exactly the manual-mode behavior and the rollback path.
- **Automatic mode**:
  - node is deterministic primary/standby for the series **and** holds a valid
    unexpired lease with token >= observed → `Allowed`.
  - otherwise → `Denied`, reason one of `not_owner`, `no_lease`, `lease_expired`,
    `lease_fenced`. The request falls back to cache/stale/replica.

This placement (before any budget-guard / scheduler admission) guarantees the
provider call is never made by a node that is not the proven owner.

---

## 5. Transport (real peer boundary)

Witness nodes serve the lease control plane over the **existing authenticated
peer boundary** (`/internal/v1/lease/peek` and `/internal/v1/lease/acquire`):

- `GET  /internal/v1/lease/peek?series=...` — read-only committed state.
- `POST /internal/v1/lease/acquire`        — fencing-guarded commit.

Both require the node credential (same `NodeAuth` as candles) and are served only
by nodes configured as witnesses (`witness_store != nil`). A non-witness refuses
lease control-plane traffic, so a candidate never mistakes a missing witness for a
vote. `cluster.RemoteWitness` adapts these endpoints to the `lease.Member`
interface; transport failures map to `LEASE_UNAVAILABLE` (counted as zero votes).

The lease control plane **never** triggers provider work — it only mutates the
committed token state. Candle serving is unchanged.

---

## 6. Fail-closed configuration

`config.Config.Validate` enforces:

- `failover_mode` must be `manual` or `automatic`. Unknown values are rejected
  (never silently enabled).
- **Automatic mode requires quorum**: `len(witnesses) + 1 >= 3` (so a strict
  majority is reachable). With fewer members, automatic mode is rejected — the
  node refuses to start rather than enable takeover without a witness majority.
- Manual mode (the default) requires no witnesses and leaves ownership exactly the
  deterministic primary.

Lease TTL defaults to `30_000 ms` if unset; the renewer sizes its cadence from it.

---

## 7. Fault-injection coverage (tests)

`internal/lease/coordinator_test.go` proves the ownership gate directly:

| Test | Proves |
|------|--------|
| `TestManualModeStaleOwnerRejected` | standby in manual mode may not take over; gate denies provider call |
| `TestAutomaticMinorityPartitionFailsClosed` | isolated node (majority-of-5 basis) fails quorum, denied provider; primary-side majority still allowed; fenced standby denied after learning newer token |
| `TestQuorumTakeoverSingleWinner` | once primary wins quorum, standby takeover is rejected (conflict/stale); only primary allowed by gate |
| `TestLeaseExpiryStopsProvider` | expired held lease → gate denies provider → stale path |
| `TestRenewalDropsLeaseOnQuorumLoss` | renewal that loses quorum drops held lease; gate denies |
| `TestDeterministicRoutingPreserved` | manual path still yields exactly one owner; routing version unchanged = no takeover |
| `TestFencingTokenMonotonic` | token forward-only; replay/lower rejected; higher token from another holder rejected *while unexpired* (split-brain guard), accepted only after expiry |
| `TestGateErrorIsTyped` | denial is a typed `apperror.Error` the data plane branches on |

All tests pass under `go test -race ./...`.

---

## 8. Operational runbook

### 8.1 Default state (manual failover)

- `failover_mode: manual`. No `lease` block required.
- Ownership is the deterministic primary from the routing table. The standby
  serves only as a peer fallback for already-cached/stale data.
- To fail over: bump `routing_version` and adjust weights/node list so the new
  primary wins the rendezvous hash, redeploy all nodes together (config-hash
  pinned). This is the documented, safe, fail-closed switch.

### 8.2 Enabling automatic failover

1. Set `failover_mode: automatic`.
2. Add at least **2 witness nodes** to `cluster.witnesses` (each with `id` and
   private `peer_url`), and the local node. Total membership must be `>= 3` or
   the node refuses to start.
3. (Optional) set `limits.lease_ttl_ms` (default 30000).
4. Redeploy. Each node logs `automatic failover enabled` with witness count.
5. The primary holds a lease per series (token 1) and the renewer keeps it valid.
   On primary crash, the standby wins the next takeover with token 2 once it
   reaches quorum with the witnesses.

### 8.3 Partition / crash behavior

- **Primary crashes**: lease expires after TTL; standby takes over via quorum
  (token increments). No gap longer than TTL without provider data; fallback to
  stale during the gap.
- **Standby partitioned from witnesses**: it cannot reach quorum, so it never
  takes over and never calls the provider. The primary (if alive) keeps ownership.
- **Witness majority lost**: no new takeovers can commit; current holders keep
  serving until their lease expires, then they fail renewal and drop the provider
  right (fail-closed) — they do not become split-brain owners.
- **Lease store fully degraded**: equivalent to "no quorum" → all nodes fall back
  to stale; deterministic manual ownership remains the safe steady state.

### 8.4 Rolling back to manual

Set `failover_mode: manual` and redeploy. No lease logic runs; ownership returns
to the deterministic primary. This is always safe and requires no token reset.

### 8.5 Observability

- Gate denials are logged at `WARN` with reason `not_owner | no_lease |
  lease_expired | lease_fenced`.
- Renewal failures are logged at `WARN` with the series key.
- Watch for repeated `lease_expired` on the primary (renewal failing) or
  `lease_fenced` on a standby (it tried to take over while the primary was alive —
  investigate the partition).

---

## 9. Files

- `internal/lease/store.go` — `NodeStore`, `WitnessStore`, fencing state machine.
- `internal/lease/coordinator.go` — `Coordinator`: `Takeover`, `Renew`, quorum.
- `internal/lease/gate.go` — `Gate`: `MayCallProvider` enforcement decision.
- `internal/lease/wire.go` — HTTP wire types (`AcquireRequest`, `StateEnvelope`).
- `internal/lease/coordinator_test.go` — ownership-gate fault tests.
- `internal/peerapi/handler.go` — `/internal/v1/lease/*` witness endpoints.
- `internal/cluster/client.go` — `RemoteWitness`, `LeasePeek`, `LeaseAcquire`.
- `internal/service/service.go` — `LeaseGate` consulted in `refresh`.
- `internal/config/config.go` — `failover_mode`, `witnesses`, `lease_ttl_ms`,
  fail-closed validation.
- `cmd/gateway/main.go` — composition root + background renewer.
