**STRATEGY STANDARD**

This document defines the institutional strategy interface and the sandbox used to run strategies safely.

Key constraints
- Strategies produce SIGNALS ONLY (no orders, no broker calls, no execution APIs).
- Strategies are stateless during execution. Any state must be persisted by the orchestrator.
- Strategies receive only sanitized inputs: `StrategyInput` (features, prices, regime flags).

Core types (in `octa_core.strategy`)
- `StrategySpec` — `id`, `universe`, `frequency`, `risk_budget`.
- `StrategyInput` — `timestamp`, `features`, `prices`, `regime`.
- `StrategyOutput` — `exposures`, `confidence`, `rationale`.

Sandbox (`octa_core.sandbox.StrategySandbox`)
- Runs strategies in a separate process with configurable time and memory limits.
- Validates outputs (no forbidden keys, exposures in [-1,1], confidence in [0,1], symbols in universe).
- Optionally audits inputs and outputs using the ledger API.

Baseline strategy
- `octa_strategies.baseline_trend.run_strategy` — volatility-scaled trend-following using simple moving averages.

Usage
- Implement strategies as a pure function `fn(spec, inp, state) -> StrategyOutput`.
- Execute via `StrategySandbox.run(fn, spec, inp, ledger_api=...)`.
