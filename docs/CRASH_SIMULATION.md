Crash Simulation

Purpose

This module provides deterministic, auditable simulations of extreme market crashes for war-game exercises. It focuses on scenarios where market moves exceed historical norms, liquidity assumptions collapse, and the risk system must dominate to protect the fund.

Scenarios

- 1987-style intraday crash: single-session deep drop across equity positions with rapid liquidity reduction.
- 2008 liquidity freeze: markets become illiquid; price moves are amplified by forced selling and margin effects.
- Correlation→1: assets move perfectly correlated; systemic moves induce margin and liquidity collapse.

Assumptions & Hard Rules

- Crashes intentionally exceed historical norms (shocks drawn from aggressive ranges).
- Liquidity reduces sharply in each scenario, limiting sell capacity.
- The `SimpleRiskSystem` enforces reductions and can engage a kill-switch if expected loss exceeds 2x the configured risk limit.

Integration Notes

- Each run returns a `MarketContext` with `audit_log` entries capturing actions and decisions.
- Results include a deterministic hash of key outputs suitable for recording as audit evidence.
- Use deterministic `seed` values during exercises for replayability.

Usage

Instantiate `MarketCrashSimulator()` and call `simulate(scenario, context_payload, seed=...)` where `context_payload` maps to `MarketContext` fields: `positions`, `prices`, `exposure`, `liquidity`, `risk_limit`.

Example

See tests in `tests/test_market_crash.py` for canonical usage patterns.
