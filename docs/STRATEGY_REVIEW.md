# Strategy Review & Evidence Pack

Purpose
-------
Produce an immutable, auditable evidence pack used for governance review and decision making.

Contents
--------
- `performance history`: ledger events for the strategy (ordered)
- `health metrics`: health scorer explain output (if available)
- `regime performance`: risk and regime-related metrics
- `risk incidents`: ledger events with incident/gate names
- `capacity usage`: capacity params and current AUM

Immutability & Reproducibility
--------------------------------
The `pack_id` is the SHA256 of the canonical JSON payload (sorted keys). Re-generating with the same inputs yields the same `pack_id`.

Generation
----------
Use `EvidencePackBuilder(registry, ledger, ...).generate(strategy_id, extra=...)`.
