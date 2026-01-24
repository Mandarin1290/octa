Capital Flows
=============

Overview
--------
`CapitalFlows` manages subscriptions and redemptions with enforced settlement delays and liquidity checks.

Hard Rules
----------
- No capital movement during active trading windows — attempts to `redeem` while trading is active raise `TradingWindowActive`.
- Redemptions are only accepted if liquid assets are sufficient at request time; final settlement performs a last liquidity/balance check.

Key Concepts
------------
- `settlement_delay_days` — number of calendar days between request and settlement.
- `liquid_assets` — cash available to satisfy redemptions.
- `pending` — list of pending subscription/redemption requests; processed by `process_settlements`.

API
---
- `CapitalFlows(settlement_delay_days=2, initial_liquid_assets=0.0)` — create manager.
- `set_trading_window(active: bool)` — toggle trading window.
- `subscribe(investor, amount, now=None)` — request a subscription (settles after delay).
- `redeem(investor, shares, nav_per_share, now=None)` — request a redemption (value=shares*nav); raises if trading window active or insufficient liquidity.
- `process_settlements(now=None)` — process due settlements; returns list of settled `PendingRequest`s.
- `get_liquidity()` and `get_balance(investor)` — read state.

Audit & Determinism
-------------------
All actions append a `FlowRecord` with an evidence hash using canonical JSON + SHA-256.

Usage Notes
-----------
- Caller must call `process_settlements` periodically (e.g., end-of-day) to apply pending movements.
- For production, integrate `liquid_assets` with real accounting ledgers and use a persisted audit store.
