# Hypothesis Registry

This registry stores formalized hypotheses suitable for deterministic evaluation
in the alpha pipeline.

Required fields
---------------
- `hypothesis_id` — unique identifier (generated if not provided).
- `economic_intuition` — concise economic rationale.
- `expected_regime` — regime where hypothesis should work.
- `expected_failure_modes` — how the hypothesis may fail.
- `risk_assumptions` — assumptions required for safety/risk.
- `test_spec` — a `dict` describing how the hypothesis is to be tested (required to ensure testability).

Immutability
------------
Registered hypotheses are frozen and cannot be modified; to change a hypothesis
one must register a new version with a new id.

Usage
-----
Create a `HypothesisRegistry` and call `register(...)` with the required fields.
