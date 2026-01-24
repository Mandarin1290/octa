# Alpha Crowding & Saturation

Purpose
- Detect internal saturation and crowding among alphas and reduce marginal value for crowded strategies.

Key features
- `pairwise_overlaps`: cosine similarity between exposure vectors.
- `crowding_index`: average overlap of each alpha with the rest (0..1).
- `diminishing_multiplier`: smooth diminishing returns curve using a tunable threshold and exponent.
- `apply_crowding_penalties`: apply multipliers to base utilities to produce adjusted utilities.

Usage
- Compute base utilities (e.g., via `risk_adjusted_utility`) then call `apply_crowding_penalties` with `CrowdingProfile` inputs.
- Configure `threshold` and `exponent` to tune sensitivity and curve steepness.

Notes
- Crowding index near 1 means near-identical exposures and strong crowding.
- For production, combine with `arbitration` and `competition` so crowding reduces allocations deterministically and is audited.
