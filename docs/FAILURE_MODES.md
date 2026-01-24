# Failure Modes Tracking

This module records observed failure modes for hypotheses and compares them
against a declared taxonomy. It ensures deviations from expected failure modes
are logged for review and auditing.

Key features
------------
- `FailureModeRegistry` holds the canonical taxonomy and stores `FailureEvent`s.
- `observe(hypothesis_id, observed_modes, details)` records an event and returns
  the logged `FailureEvent` including any `unexpected` modes.
- `get_events_for_hypothesis(hypothesis_id)` helps link observed failures back
  to the `Hypothesis` in the registry.

Usage
-----
Register known failure modes up front (e.g., 'mean_reversion', 'data_leak'),
then call `observe(...)` when failures are observed; review `unexpected` list
to find deviations that require triage.
Failure modes and safe defaults

- Config failure: If `octa_fabric` cannot load required settings, the system raises a `ConfigurationError` and refuses to start critical services. Default behavior: fail closed (no trading).
- Risk evaluation failure: If `octa_sentinel` cannot evaluate a rule due to internal error, it returns a blocking verdict and records an incident.
- Ledger failure: If `octa_ledger` cannot persist an audit record, components should retry with exponential backoff; if persistence remains unavailable, the system should refuse execution to preserve auditability.
- Orchestrator wiring failure: `octa_nexus` will validate all components at startup; missing dependencies result in a non-starting state.

Operators should monitor logs and the health endpoints (not implemented in this skeleton) and follow the operating playbook before enabling execution.
