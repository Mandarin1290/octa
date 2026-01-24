# Live-Prep Compliance Checklist

Overview
--------
The Live Checklist enforces a deterministic set of pre-live requirements. Live can only be enabled when the checklist passes 100%.

Checks
------
- Paper gates passed
- Shadow mode stable for >= N days
- Zero unresolved critical incidents
- Audit chain intact
- Kill-switch tested
- Capacity & liquidity stress passed

Usage
-----
Use `octa_sentinel.live_checklist.LiveChecklist` to run checks via `run_checks(ctx)` and to `enable_live()` after a successful run. Results are stored immutably and signed for provenance.
