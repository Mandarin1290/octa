Strategy Sabotage Simulation

Purpose

Simulate internal strategy failures (sabotage) and verify that detection systems isolate the faulty strategy without allowing portfolio contagion.

Sabotage types

- Runaway leverage: positions are inflated causing leverage to exceed safe thresholds.
- Inverted signals: strategy signals are flipped causing it to trade opposite to intended signals.
- Stuck positions: strategy stops trading despite active signals, leading to unmanaged exposure.

Detection & Isolation

- `StrategyMonitor` applies independent checks per strategy:
  - Leverage checks using notional/equity ratio.
  - Signal sign-agreement checks using historical `signal_history` stored in `metadata`.
  - Inactivity checks (ticks of no trades) or explicit `stuck` flag.

- On detection, the monitor sets `active = False` on the offending `StrategyContext` and logs the action. Other strategies are evaluated and left untouched to prevent contagion.

Usage

- Create `StrategyContext` instances describing strategies, including `positions`, `prices`, `cash`, `signals`, and optional `metadata['signal_history']`.
- Use `SabotageSimulator` to apply sabotage types to a target context.
- Run `StrategyMonitor.assess_and_isolate([ctx1, ctx2, ...])` to detect and isolate compromised strategies.

Notes

- The module is intentionally simple and deterministic for testing. Replace heuristics with production monitoring rules and telemetry in real deployments.
