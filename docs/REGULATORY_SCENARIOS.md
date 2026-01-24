# Regulatory Scenario Simulation Mode

Purpose: simulate regulatory stress scenarios for readiness testing. Simulations are read‑only and produce findings mapped to controls.

Scenarios:

- `regulator information request` (RIR): evaluates record‑keeping controls and evidence readiness.
- `sudden rule change`: checks risk controls and change management readiness.
- `on‑site audit`: verifies audit evidence availability and control mappings.

Usage:

- Instantiate `RegulatorySimulator` and call the scenario methods with your `ControlMatrix`, `ChangeManagement` and optional evidence store.
- Each simulation returns a `ScenarioReport` with `gaps` and `mapped_controls` suitable for remediation tracking.

Limitations:

- This is a synthetic, heuristic simulator — use as guidance for governance teams, not as a compliance certification.
