# Alpha Arbitration — Redundancy Resolution

Purpose
- Prevent redundant alphas from crowding the portfolio and diluting returns.

Core rules
- Detect overlap between alpha exposures using cosine similarity (0..1).
- If an alpha's maximum overlap with already‑selected alphas exceeds the `overlap_threshold` (default 0.8), it is defunded.
- Otherwise requested capital is reduced proportionally by the crowding factor: `adjusted = requested * (1 - max_overlap)`.
- Allocation proceeds deterministically by base utility ranking (risk‑adjusted), with capital greedily assigned until exhaustion.

API
- `AlphaProfile`: input for arbitration including `exposure` vector.
- `detect_overlaps(profiles)`: returns pairwise overlap map.
- `resolve_arbitration(profiles, total_capital, overlap_threshold=0.8)`: returns allocation records with `allocated_capital`, `max_overlap_with_selected`, and `base_utility`.

Notes
- Uses `risk_adjusted_utility` from `octa_alpha.competition` to ensure consistent merit measures.
- Deterministic quantization to 8 decimals for reproducibility.
- For production, consider more elaborate marginal contribution analysis (e.g., portfolio-level risk/return optimization) and auditing via `AuditChain`.
