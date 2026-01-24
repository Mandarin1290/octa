# Feature Gates

This module implements pre-modeling checks for features. It enforces:

- Data completeness: `values` must be present and contain no nulls.
- Stationarity diagnostics: simple deterministic split-sample checks for mean/variance shifts.
- Latency feasibility: `latency_ms` must be within configured bounds.
- Leakage detection: features with high correlation to future targets are rejected.
- No undocumented transforms: `transforms` must be in an allowlist.

The `FeatureGates.check(feature_meta)` method returns a `FeatureGateResult` with `passed`, `reasons`, and `details`.
