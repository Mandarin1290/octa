# Model Risk Management (MRM) Layer

Purpose: enforce model approvals, validate model changes, and audit production usage.

Hard rules:

- Every model must have an approval state before production use.
- Model changes require validation evidence before approval.
- Production use without approval is forbidden unless explicitly overridden (overrides are logged).

API (see `octa_reg.model_risk`):

- `register_model(name, version, metadata)` — create a model inventory entry (unapproved by default).
- `propose_change(model_id, new_version)` — propose a new version (resets approval and evidence).
- `add_validation_evidence(model_id, evidence)` — attach evidence (e.g., validation report, metrics, audit evidence id).
- `approve_model(model_id, approver)` — approve model for production (requires validation evidence).
- `use_model(model_id, actor)` — enforce production use; raises if not approved.
- `override_use(model_id, actor, justification)` — allow temporary usage and log justification.

Operator guidance:

- Maintain validation evidence as structured artifacts (use `octa_reg.audit_evidence` for immutability).
- Approvals and overrides are audited and should be surfaced to governance dashboards.
