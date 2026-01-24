Capital Gates
=============

Overview
--------
`CapitalGates` provides protective controls over redemptions to prioritise portfolio survival.

Controls
--------
- Redemption gate: open/closed override that blocks all redemptions when closed.
- Side-pocket logic: a configurable fraction of any allowed redemption can be moved to a side-pocket (illiquid pool).
- Stress-based limits: a stress metric [0..1] reduces per-period redemption caps; extreme stress can nearly close redemptions.
- Survival buffer: ensures a minimum liquidity reserve as a fraction of portfolio value.

API
---
- `CapitalGates(base_limit_percent=0.2, survival_ratio=0.05, stress_sensitivity=0.9)` — create gate controller.
- `set_redemption_gate(open: bool)` — open or close redemptions.
- `enable_side_pocket(allocation_percent)` / `disable_side_pocket()` — manage side-pocket allocation.
- `set_stress_metric(value)` — set current stress (0..1).
- `evaluate_redemption(requested_value, investor_balance, liquid_assets, portfolio_value)` — returns `allowed`, `side_pocket`, `blocked`, `reason`.

Usage Notes
-----------
- Gates are authoritative: callers should consult `evaluate_redemption` prior to accepting or scheduling redemptions.
- For integration, a flow engine may use the `allowed` amount to create a pending redemption (with side-pocket bookkeeping handled separately).

Audit
-----
Every gate action is recorded with an evidence hash using canonical JSON + SHA-256.
