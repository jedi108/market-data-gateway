# Capacity-plan switch

`capacity_plan` is a startup-scoped forecast, not a second scheduler. The
existing scheduler, cache/singleflight, SWR, routing, warmup, and cluster-budget
guard remain the only runtime admission authorities.

Modes:

- `off` (default): no forecast; request behavior is unchanged.
- `shadow`: calculate a deterministic forecast and export bounded
  `gateway_capacity_plan_*` metrics. It never rejects readiness or traffic and
  never changes provider calls, scheduling, routing, or warmup.
- `enforce`: strict validation and feasibility are fail-closed at startup;
  `/v1/ready` remains unavailable for an invalid/infeasible plan. A plan must
  use the single existing TBank credential-group budget, `gateway_count: 1`,
  external sources, and exactly the reviewed enabled warmup timeframe universe.
  After readiness, the regular scheduler still controls admission.

The plan's `hard_rps` is derived only from `providers.tbank.safe_budget_per_minute`.
Node count and node budget shares never multiply capacity. Symbols, tokens, URLs,
and request IDs are not accepted as plan inputs or metric labels.

## Rollout and rollback

1. Commit a reviewed plan with `mode: shadow`.
2. Deploy only through the repository Makefile and observe complete relevant
   close cycles. Compare forecast metrics with actual scheduler queue delay and
   candle delivery/freshness metrics; forecast values are not actual deadlines.
3. Commit the reviewed change to `mode: enforce`, then deploy through Makefile.
4. Roll back with a committed configuration change to `shadow` or `off`, again
   using Makefile deployment. Never edit configuration on a server directly.

An enforce failure is intentionally a readiness/startup signal, not a fallback
provider path and not a per-request shortcut. Existing external timeframes are
not disabled by changing this plan; the plan only describes a reviewed universe.
