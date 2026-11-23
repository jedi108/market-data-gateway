# Changelog

## 2026-11-23 — fix(gateway): report funding retention (refs #1048)

- Include funding event counts before, after, and deleted in cleanup reports.

## 2026-11-23 — fix(gateway): type funding archive failures (refs #1048)

- Return typed coverage-unknown errors for rejected funding archives and assert typed service guards.

## 2026-11-23 — fix(gateway): guard diagnostic routing (refs #1048)

- Reject mismatched candle identities, report funding upstream operations separately, and test diagnostic spot fail-closed behavior.

## 2026-11-23 — fix(gateway): reject unproven pins and nondecimal funding (refs #1048)

- Fail closed without independently evidenced archive pins; validate funding rates as decimal strings and document diagnostic spot limitation.

## 2026-11-23 — test(gateway): verify funding HTTP metadata and spot (refs #1048)

- Exercise reconciled archive/API fixtures through GET and cache for 4h/8h payment availability; preserve fake-mode Binance spot trade coverage.

## 2026-11-23 — fix(gateway): fail closed for funding edge cases (refs #1048)

- Require the explicit BTC/USDT:USDT futures symbol; reject empty cached funding windows and peer funding transport, and verify 4h/8h HTTP event metadata.

## 2026-11-23 — fix(storage): retain funding events within cleanup horizon (refs #1048)

- Apply SQLite retention to funding events while preserving the latest event per series and trimming coverage as before.

## 2026-11-23 — test(gateway): verify diagnostic funding bounds (refs #1048)

- Document the diagnostic GET restrictions and test identity and cached result limits.

## 2026-11-23 — feat(gateway): serve diagnostic funding GET (refs #1048)

- Route explicitly labeled reconciled funding through scheduler, owner gate, cache and SQLite; enable a diagnostic Binance mode for the pinned BTCUSDT archive window. Non-owner peer funding and mark remain unavailable and fail closed.

## 2026-11-23 — feat(storage): persist typed funding events (refs #1048)

- Add a funding-event union to the existing cache and SQLite snapshot path, limited to reconciled diagnostic series. This is an intermediate step; funding is not yet wired to GET or the provider routing path.
