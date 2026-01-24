# Multi-Asset Performance Attribution

Overview
--------
This module attributes portfolio PnL across asset classes and strategies, separates FX translation effects and reports hedge contribution. Reconciliation is enforced.

Inputs
------
- `pnl_by_strategy_asset`: mapping `strategy -> asset_class -> {'pnl_local', 'currency'}`.
- `fx_rates`: mapping currency -> rate to base currency (e.g., EUR:1.1 means 1 EUR = 1.1 BASE).
- `hedges`: optional mapping with same structure describing hedge PnL in local currency.

Outputs
-------
- `asset_class_pnl`: aggregated PnL per asset class in base currency.
- `strategy_asset_matrix`: `strategy -> asset_class -> pnl_base` matrix.
- `fx_translation_effect`: amount added by currency translation (base - local sum).
- `hedge_contribution`: total hedge PnL in base currency (separate line).
- `total_pnl`: sum of strategy matrix plus hedge contribution.
- `reconciles`: boolean indicating exact numeric reconciliation.

Reconciliation
--------------
The function enforces:

total_pnl == sum(strategy_asset_matrix entries) + hedge_contribution

and exposes `fx_translation_effect` so users can separate currency translation from local performance.

Usage
-----
Call `octa_ledger.multi_asset_attribution.attribute_pnl()` during end-of-day attribution pipelines and include outputs in audit ledger.
