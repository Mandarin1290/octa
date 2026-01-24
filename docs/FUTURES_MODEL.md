# Futures Model

Overview
--------
This module provides institutional-grade futures support: explicit contract specs, deterministic roll logic for execution, and basis risk tracking. Contract metadata is authoritative; missing metadata freezes trading for the instrument.

Key Concepts
------------
- `FuturesContract` contains symbol, root, expiry, multiplier, tick size, currency, and margin fractions.
- `RollManager` implements deterministic forward-linked roll logic (roll window and open-interest trigger) and a back-adjust helper for research.
- `compute_basis` measures divergence between spot and future (per-multiplier).

Integration Notes
-----------------
- If `ContractRegistry.enforce_exists()` fails, trading for that instrument is frozen via `sentinel.set_gate(3, ...)`.
- Margin calculations use `FuturesContract.multiplier` and contract margin fractions; integrate with `octa_core.margin` by calling `ContractRegistry.margin_required()`.
