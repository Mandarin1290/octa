# Stress Testing Harness

This document describes the historical and parametric stress testing harness implemented in `octa_sentinel.stress_harness` and artifact storage in `octa_atlas`.

Principles
----------
- No lookahead: historical runner uses only past returns provided for the requested window.
- Deterministic: runs are reproducible given identical inputs and versions.
- Audit & provenance: each run writes an `AuditEvent` to the ledger and saves a `RiskProfile` artifact in Atlas.

Usage
-----
- `StressHarness.run_historical(portfolio_id, positions, returns, window_name, version)`
- `StressHarness.run_parametric(portfolio_id, positions, shocks, version)`
