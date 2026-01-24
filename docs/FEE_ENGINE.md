Fee Engine
===========

Overview
--------
This module implements a hedge-fund-grade fee engine with:

- Management fee accrual (annual rate, pro-rated by days)
- Performance fee accrual only on gains above the High-Water-Mark (HWM)
- Strict HWM tracking and update on performance fee crystallization
- Crystallization rules: accrued fees move to `payable` and performance crystallization bumps HWM

Hard Rules
----------
- Fees must never influence trading decisions. The engine only observes NAVs and records fee amounts; applying fees to assets is the responsibility of the caller.
- High-Water-Mark is strictly enforced: performance fees only on NAV-per-share above the current HWM.

API
---

- `FeeEngine()` — manager for multiple share classes.
- `add_share_class(share_class, initial_hwm, mgmt_rate_annual, perf_rate)` — register a share class.
- `accrue_management(share_class, nav_per_share, days)` — accrue management fee pro-rated for `days`.
- `accrue_performance(share_class, nav_per_share)` — accrue performance fee only on NAV above HWM.
- `crystallize(share_class, nav_per_share)` — crystallize accrued fees, move to payable, and (for performance) bump HWM to `nav_per_share`.
- `payable(share_class)` — current payable balance for the manager for that share class.
- `snapshot_audit()` — returns minimal audit snapshot per share class.

Determinism & Audit
--------------------
Each fee action records an append-only `audit_log` entry with an evidence hash computed using canonical JSON (sorted keys, compact separators) and SHA-256.

Usage Notes
-----------
- Caller must ensure NAV-per-share inputs are the authoritative source; fees are calculated against NAV-per-share as provided.
- Apply crystallized fees to accounts separately (the engine only tracks `payable`).
