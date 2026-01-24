**Drawdown Recovery Playbook**

This document describes the automated, rule-based drawdown recovery engine implemented in `octa_sentinel.drawdown_playbook`.

Rules (summary)
- Ladder:
  - 2% DD: reduce gross by 10% across strategies (mild)
  - 5% DD: reduce gross by 25% and freeze new risk for weakest strategies
  - 8% DD: reduce gross by 50% and flatten high-impact assets
  - 10%+ DD: configurable kill-switch — full flatten and incident
- Re-risking requires:
  - volatility normalized relative to baseline
  - correlation stress below threshold
  - no critical incidents in lookback window
  - paper gates passing

Outputs
- `compression`: per-strategy target scaling (0..1)
- `freeze_list`: strategies for which new risk should be frozen
- `flatten_assets`: which asset groups to flatten
- `rationale`: list of human-readable reasons
- `re_risk_allowed`: boolean

Audit
- All actions should be written to the ledger as `drawdown_playbook.evaluation` events with `rationale` and `compression` payload.
