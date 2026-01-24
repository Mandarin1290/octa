Liquidity Drain & Fire-Sale Simulation

Purpose

Simulate sudden liquidity evaporation and test protections that prioritize capital preservation over PnL during fire-sales.

Scenarios

- `spread_explosion`: bid-ask spreads explode and slippage increases.
- `zero_bid`: certain assets momentarily have zero bids (cannot be sold).
- `forced_liquidation`: cascading margin/forced selling increases slippage over time.

Protections

- `LiquidityProtector` caps per-step liquidation fraction and enforces a `capital_preservation_floor` to stop sales that would endanger capital.
- Simulations produce audit logs and deterministic result hashes.

Usage

Instantiate `LiquidityDrainSimulator()` and call `simulate(scenario, ctx_payload, seed=..., steps=...)` with `ctx_payload` matching `LiquidityContext` fields (`positions`, `prices`, `cash`, `liquidity`, `spread`).
