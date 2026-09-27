# ADR-007: Gateway capacity planning uses credential-group budgets

Status: accepted

## Decision

Capacity is measured per provider credential group, not per process, host, IP,
proxy, or account. A group shared by several gateway nodes has one
`safe_budget_per_minute`; adding nodes cannot manufacture provider capacity or
repair a group-level deficit.

The planner converts the reviewed budget to `hard_rps = safe_budget_per_minute /
60`. `effective_rps` is hard RPS multiplied by explicit target utilization.
The retry factor is forecasting headroom for average demand only. Boundary and
EDF deadline checks use the requested workload without retry headroom, avoiding
double counting. External sources consume capacity; derived sources are
capacity-neutral in this pure what-if model, and do not authorize runtime
aggregation or source switching.

The package in `internal/capacity` is deterministic, standard-library-only and
is not admission control. The existing scheduler, cache/SWR, routing and
budget guard remain the sole authority before a provider attempt. Overflow and
pathological scheduling cycles fail closed through sentinel errors.
