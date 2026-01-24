# Paper Deployment Automation

This module automates deterministic paper deployments into a `PAPER` lifecycle
state. Deployments are reproducible: the deployment id is a deterministic
hash of the `hypothesis_id` and the `signal` value.

API
---
- `LifecycleEngine` — minimal store for lifecycle records.
- `PaperDeploymentManager(lifecycle_engine, audit_fn)` — create a manager.
- `deploy(hypothesis_id, signal, paper_capital)` — registers deployment with
  state `PAPER` and emits `paper.deployed` audit event.

Hard rules
----------
- No manual deployment: this manager is designed to be called by orchestration
  only (e.g., pipeline `paper_deploy` stage).
- Lifecycle integration mandatory: deployments are stored in `LifecycleEngine`.
- Audit linkage: `paper.deployed` event is emitted when possible.
