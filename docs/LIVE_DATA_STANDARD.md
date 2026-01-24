# LIVE DATA STANDARD

Overview
--------
Live market data is treated as a Tier-1 input and must be validated with the same rigor as historical data. Any degradation or inconsistency in live feeds triggers a fail‑closed behavior and notifies the Sentinel to freeze trading where necessary.

Rules
-----
- All timestamps must be consistent in kind (either timezone-aware UTC or naive interpreted as UTC). Mixing kinds is strictly forbidden.
- Each incoming bar must include: `instrument`, `timestamp`, `open`, `high`, `low`, `close`, and optionally `volume`.
- Feed quality checks include staleness (latency), gaps between updates, and out-of-order timestamps.
- On any quality failure, the feed should notify Sentinel using `set_gate(level, reason)` with level >= 2.

Metrics
-------
- `latency_seconds`: now() - bar.timestamp
- `gap_seconds`: time since last bar for instrument
- `heartbeat`: last seen timestamp per instrument

Integration
-----------
Use `octa_stream.live_feed.LiveFeed` and `octa_stream.live_quality.LiveQualityChecker` to implement ingestion points. Ensure `audit_fn` is provided to record events into the ledger for provenance.
