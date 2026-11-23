# Changelog

## 2026-11-23 — fix(storage): retain funding events within cleanup horizon (refs #1048)

- Apply SQLite retention to funding events while preserving the latest event per series and trimming coverage as before.

## 2026-11-23 — test(gateway): verify diagnostic funding bounds (refs #1048)

- Document the diagnostic GET restrictions and test identity and cached result limits.

## 2026-11-23 — feat(gateway): serve diagnostic funding GET (refs #1048)

- Route explicitly labeled reconciled funding through scheduler, owner gate, cache and SQLite; enable a diagnostic Binance mode for the pinned BTCUSDT archive window. Non-owner peer funding and mark remain unavailable and fail closed.

## 2026-11-23 — feat(storage): persist typed funding events (refs #1048)

- Add a funding-event union to the existing cache and SQLite snapshot path, limited to reconciled diagnostic series. This is an intermediate step; funding is not yet wired to GET or the provider routing path.
