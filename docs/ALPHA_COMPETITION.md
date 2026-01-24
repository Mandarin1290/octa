# Alpha Competition — Internal Marketplace

Overview
- Capital is scarce and contested. Alphas compete for virtual capital allocations.
- Allocation is never guaranteed; the engine awards capital based on risk-adjusted merit and declared bids.

Key concepts
- Submission: an alpha's virtual request including `requested_capital`, `expected_return`, `volatility`, `base_confidence`, and optional `bid_price`.
- Utility: risk-adjusted merit computed as `(expected_return * base_confidence) / (volatility * risk_aversion)`.
- Ranking: alphas are ranked by utility (descending). Tie-breakers: higher `bid_price`, then `alpha_id` (deterministic).
- Allocation: greedy fill highest-ranked requests until the total capital pool is exhausted. Partial fills allowed.

Explainability
- Each allocation record includes: `alpha_id`, `requested_capital`, `allocated_capital`, `utility`, and `bid_price`.

Determinism & Audit
- All computations use `Decimal` and quantize to 8 decimals to ensure reproducibility.
- Sorting and tie-breaking are deterministic to avoid non-deterministic allocations.

API
- `Submission` dataclass: inputs for an alpha.
- `run_competition(submissions, total_capital) -> List[allocations]` returns explainable allocation records.

Guidance
- Use conservative `base_confidence` and realistic `volatility` inputs.
- For production, wire competition events to the `AuditChain` for a tamper-evident record.
