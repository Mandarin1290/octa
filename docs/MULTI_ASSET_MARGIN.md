# Multi-Asset Capacity & Margin Aggregation

Overview
--------
This module aggregates exposures and computes worst-case margin across multiple asset classes to ensure leverage is governed by the most conservative requirement.

Principles
----------
- Leverage is aggregated across asset classes, not siloed.
- Worst-case margin across stress scenarios governs available capital and leverage.
- Sentinel gates freeze trading when portfolio-level margin breach is detected.

Algorithm
---------
1. Compute per-class margins using configurable `margin_rates`.
2. Apply a set of stress scenarios (multiplicative factors per-class) and compute scenario margins.
3. The worst-case margin is the maximum across scenarios.
4. Stress-adjusted leverage = total_exposure / max(0, capital - worst_case_margin).
5. If worst_case_margin > capital or leverage exceeds a configured `leverage_limit`, call `sentinel_api.set_gate(3, reason)` and audit.

Integration
-----------
- Use `octa_core.multi_asset_risk.MultiAssetRiskEngine.assess_and_enforce()` in periodic risk scans and pre-trade checks.

Notes
-----
- The scenario set is intentionally small and illustrative. For production, use scenario libraries derived from historical extreme moves and reverse stress testing.
