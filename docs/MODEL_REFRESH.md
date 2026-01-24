# Model & Data Refresh Discipline

Purpose
-------
Control retraining and data refresh cycles with explicit governance approval, validation gates and rollback readiness.

Hard Rules
----------
- No silent retraining: retrain must be approved using the approval API.
- Governance approval mandatory: `approve_retrain` must be called before `execute_retrain`.

Controls
--------
- `request_retrain(model_id, trigger, proposer)` — register a retrain request.
- `approve_retrain(model_id, approver)` — governance approves the retrain.
- `validate_model(model_id, candidate_version, metrics)` — validation gate used in `execute_retrain`.
- `rollback(model_id)` — revert to previous version if `rollback_ready`.

Audit
-----
- Every action appends to `audit_log` with a canonical evidence hash for governance records.
