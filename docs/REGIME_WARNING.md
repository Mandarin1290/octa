# Regime Shift Early Warning System

Purpose
-------
Provide early, conservative warnings for regime shifts (volatility structure changes,
correlation breakdowns and macro shocks) to enable governance and risk teams to act
before losses compound.

Signals
-------
- volatility structure: increase in realized or implied volatility
- correlation breakdown: meaningful drop in average cross-asset correlation
- macro shock indicators: external shock score (0..1)

Design Principles
-----------------
- Early-warning, not prediction: system favors low false-positive rate.
- Conservative bias: warnings require multiple signals to align.
- Append-only audit: every metric record, evaluation and warning produces an evidence hash.

API
---
- `RegimeWarningSystem.record_metrics(date, strategy, volatility, avg_corr, macro_shock)`
- `RegimeWarningSystem.evaluate(strategy) -> Optional[RegimeWarning]`
- `RegimeWarningSystem.get_alerts()`
- `RegimeWarningSystem.get_audit()`
