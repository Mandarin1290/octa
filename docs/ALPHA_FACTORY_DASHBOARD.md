# Alpha Factory Dashboard

This dashboard aggregates deterministic metrics from the alpha factory:

- Active hypotheses (from `HypothesisRegistry`).
- Pipeline stage counts (from `AuditChain` event logs).
- Rejection reasons (extracted from pipeline audit events ending with `.rejected` or `.failed`).
- Paper deployment flow (from `LifecycleEngine` registrations).
- Failure statistics (from `FailureModeRegistry`).

The `AlphaFactoryDashboard.summary()` method returns a fully reconciled
dictionary of these metrics suitable for institutional review.
