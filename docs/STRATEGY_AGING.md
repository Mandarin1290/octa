# Strategy Aging Model

Purpose
-------
Explicit, transparent rules to raise evidence requirements as strategies age.

Key points
----------
- Tracks time since a strategy entered `LIVE` and maps to tiers: `YOUNG`, `MATURE`, `OLD`.
- Tiers adjust escalation thresholds (warn/derisk/suspend) by configurable multipliers.
- Aging does not imply decay; it only tightens thresholds to require stronger evidence for continued allocation.
- `AgingEngine.check_and_escalate()` integrates with `RiskBudgetEngine` and `sentinel_api` to emit non‑blocking gates when tightened thresholds are exceeded.

Configuration
-------------
Set `AgingConfig` multipliers and day boundaries to tune how aggressively thresholds tighten over time.
