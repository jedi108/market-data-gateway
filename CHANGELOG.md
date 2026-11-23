# Changelog

## 2026-11-23 — feat(storage): persist typed funding events (refs #1048)

- Add a funding-event union to the existing cache and SQLite snapshot path, limited to reconciled diagnostic series. This is an intermediate step; funding is not yet wired to GET or the provider routing path.
