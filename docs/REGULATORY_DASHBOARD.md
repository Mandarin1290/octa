# Regulatory & Audit Dashboard

Purpose: provide a deterministic compliance and audit overview with live linkage to controls and evidence.

Fields:

- `control_coverage`: objectives and their controls with owner, testable flag and evidence links.
- `evidence_status`: mapping from control ids to evidence ids and total evidence count.
- `open_findings`: open remediation tasks from postmortem reviews (incident id, reviewer, tasks).
- `model_approvals`: list of models with approval status and approver.
- `change_activity`: change requests and recent change audit events.

Determinism and rules:

- All lists are deterministically ordered (sorted by ids or names).
- Dashboard contains factual state only — no subjective scoring.
- Live linkage: supply live manager instances (control matrix, evidence store, postmortem manager, model risk, change mgmt) to `RegulatoryDashboard`.

Usage:

- Instantiate `RegulatoryDashboard` with the managers and call `snapshot()` to get a JSON-serializable overview for compliance teams.
