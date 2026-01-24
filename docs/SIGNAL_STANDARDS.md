# Signal Construction Standards

This document defines the rules for building deterministic, auditable signals.

Hard rules
----------
- Signals must be bounded to [-1, 1].
- Signal meaning and transforms must be documented (no implicit transforms).
- No implicit leverage: by default sum(abs(signals)) must not exceed 1.

API
---
- Use `SignalBuilder(values)` to create a builder.
- Call a normalization method (`normalize_minmax()` or `normalize_zscore()`).
- Encode direction via `encode_direction(directions=None)` (explicit list or inferred).
- Apply confidences via `apply_confidence([0..1])`.
- Call `enforce_bounds()` to ensure rules are satisfied; it raises on violations.
