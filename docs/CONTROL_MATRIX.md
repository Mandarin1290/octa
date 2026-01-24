# Control Objective & Control Matrix

Purpose: define formal control objectives and map them to testable, owned controls with evidence linkage.

Hard rules:

- Every regulatory objective must map to at least one control.
- Controls must be owned (an owner string) and testable.
- Controls must include evidence links that demonstrate execution and testing.

Key concepts:

- `ControlObjective`: high-level objective (id, description, domains).
- `Control`: an implementable control (id, objective_id, description, owner, frequency, evidence_links, testable).
- `ControlMatrix`: registry and validator for objectives and controls.

Usage:

1. Create `ControlObjective` entries and register them with `ControlMatrix.register_objective()`.
2. Create `Control` entries (must reference an existing objective) and register with `ControlMatrix.register_control()`.
3. Use `flag_missing_controls()` to find objectives without controls.
4. Use `enforce_ownership_raise()` to fail fast if any control lacks an owner.
5. Attach evidence with `link_evidence(control_id, link)` for auditability.

Testing & automation:

- Controls must be testable (`testable=True`) to be considered ready for automated checks.
- The `ControlMatrix` provides `validate_testable_controls()` to find controls that need test coverage.
