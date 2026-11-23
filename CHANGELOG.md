# Changelog

## 2026-11-23 — feat(gateway): serve diagnostic funding GET (refs #1048)

- Route explicitly labeled reconciled funding through scheduler, owner gate, cache and SQLite; enable a diagnostic Binance mode for the pinned BTCUSDT archive window. Non-owner peer funding and mark remain unavailable and fail closed.

## 2026-11-23 — feat(storage): persist typed funding events (refs #1048)

- Add a funding-event union to the existing cache and SQLite snapshot path, limited to reconciled diagnostic series. This is an intermediate step; funding is not yet wired to GET or the provider routing path.
