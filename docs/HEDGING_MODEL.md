# Cross-Asset Hedging Model (Equity ↔ Rates ↔ FX)

Overview
--------
This module provides a small, explainable hedging engine intended for use inside OCTA runtime pre-trade and risk workflows.

Design principles
-----------------
- Hedging reduces risk, not increases it.
- Hedges are explainable (simple beta-based ratios) and bounded by regime scaling.
- Ineffective hedges are flagged via the sentinel for human review and automatic reduction.

Mappings
--------
- Equity beta hedge: `EQ` ↔ `EQ_FUT` (beta from returns covariance).
- Duration hedge: `RATES` ↔ `RATES_FUT` (beta from bond/rate returns).
- FX hedge: `FX` ↔ `FX_PROXY` (spot/forward proxy correlation).

Key algorithm
-------------
1. Compute beta = cov(asset, hedge) / var(hedge).
2. Hedge ratio = beta * regime_scale (regime_scale ∈ {calm:1.2, normal:1.0, volatile:0.75, stress:0.5}).
3. Position = -ratio * exposure (sign chosen to reduce exposure direction).
4. Evaluate effectiveness by computing variance reduction:
   reduction = 1 - Var(net PnL) / Var(unhedged PnL).
5. If reduction < threshold (default 0.01) then call `sentinel_api.set_gate(3, reason)` and audit.

Integration
-----------
- Use `octa_core.hedging.HedgeEngine` with `audit_fn` and `sentinel_api` hooks.
- Call `assess_and_enforce()` during pre-trade risk checks to both compute positions and enforce sentinel gates when necessary.

Notes & Limitations
-------------------
- This is an explainable, prototype-grade engine. For production, replace with exposure-normalized beta estimation, term-structure aware duration hedges, and PKI-backed audit logs.
- Hedge sizing here is per-unit-exposure; a downstream allocator should convert to contract counts using contract definitions and notional sizes.
