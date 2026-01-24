Failure Cascade Simulation

Purpose

Simulate cascading failures across systems (data → strategy → execution and broker → reconciliation → NAV) and verify that circuit breakers and isolation prevent unchecked propagation.

Design

- `FailureCascadeSimulator` composes detectors, monitors and the OMS.
- `CircuitBreaker` trips when failures exceed a threshold and stops further routing.
- `simulate_data_strategy_execution` inspects primary feed, isolates affected strategies, and prevents routing to isolated strategies.
- `simulate_broker_reconciliation` models reconciliation mismatches and prevents NAV updates when mismatches exceed threshold.

Usage

- Use `FailureCascadeSimulator()` to orchestrate cross-system failure chains and validate containment.
