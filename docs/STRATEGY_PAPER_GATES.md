# Strategy Paper Gates

Overview
--------
Defines quantitative, reproducible gates that a strategy must pass in `PAPER` before promotion to `SHADOW`.

Gates (default thresholds)
---------------------------
- Minimum paper runtime: `runtime_days >= 7`
- Maximum drawdown: `max_drawdown <= 0.10` (10%)
- Risk-adjusted return: `sharpe >= 0.5`, `sortino >= 0.7`
- Slippage stability: `slippage_diff <= 0.02` (absolute)
- No critical incidents: `incidents == 0`
- Correlation vs existing strategies: `max_corr <= 0.6`

Usage
-----
Use `octa_strategy.paper_gates.PaperGates.evaluate(metrics)` for reproducible gate evaluation. Call `promote_if_pass(lifecycle, metrics, doc)` to perform the documented transition to `SHADOW` if and only if all gates pass.
