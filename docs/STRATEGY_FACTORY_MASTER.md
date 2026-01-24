# Strategy Factory Master Dashboard

Purpose
- Single source of truth for alpha inventory, regime-adjusted scoring, capital allocation, crowding indicators and governance interventions.

Features
- `alpha_inventory`: reconciles candidate metadata and governance state.
- `regime_adjusted_scores`: calls `score_alpha` per alpha for an explainable score.
- `allocation_map`: deterministic weights from `optimize_weights`.
- `crowding`: per-alpha crowding index.
- `governance`: audit log and veto list.

Reconciliation
- Dashboard includes `total_weight` and uses deterministic, Decimal quantization across subsystems to ensure reconcilability.

Usage
- Instantiate `StrategyFactoryMaster(governance=Governance())` and call `build_dashboard(...)` with candidate list and signal inputs.
