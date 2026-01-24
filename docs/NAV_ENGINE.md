**NAV Engine (Deterministic, Audit‑Grade)**

Kurzbeschreibung
- `octa_accounting.nav_engine.NAVEngine` berechnet deterministic Institutionelles NAV mit realized/unrealized PnL, fees und cash movements.
- Designed for reproducible historical replay and canonical evidence hashing.

Funktionen
- `deposit(amount)`, `withdraw(amount)` — Cash movements.
- `record_trade(symbol, qty, price, fee=None)` — Updates positions, cost basis, realized PnL and cash.
- `update_market_price(symbol, price)` — Updates market prices used for unrealized PnL.
- `accrue_fee(amount)` — Accrues fees and reduces cash.
- `compute_nav()` — Returns deterministic snapshot including `report_hash`.
- `replay_history(events)` — Reconstruct state by replaying recorded events deterministically.

Audit
- `audit_log` contains append‑only events with timestamps; `history` holds event stream usable for replay.

Usage
```py
from octa_accounting.nav_engine import NAVEngine

e = NAVEngine()
e.deposit(100000)
e.record_trade("A", 100, 100)
e.update_market_price("A", 110)
report = e.compute_nav()
print(report)
```
# NAV Engine (Deterministic)

Overview
--------
The `NAVEngine` provides deterministic, auditable Net Asset Value calculations per share class. It reconciles class `cash` + `assets` against `shares_outstanding` and emits a reconciliation audit event.

Rounding
--------
- Currency totals are rounded to 2 decimal places using `ROUND_HALF_EVEN`.
- NAV per share is rounded to 6 decimal places using `ROUND_HALF_EVEN`.

API
---
- `NAVEngine(audit_fn=None)`
- `compute_nav(share_classes, period='daily')` — returns mapping of class_id -> `{total, nav_per_share}`.

Determinism
-----------
- Inputs must be deterministic and audited; the engine processes classes in sorted order to ensure reproducible output.
