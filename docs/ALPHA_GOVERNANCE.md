# Alpha Governance & Oversight

Purpose
- Provide lightweight governance controls to approve, veto or override alpha selection decisions while ensuring all interventions are auditable.

Core rules
- `submit_for_approval(alpha_id)` records submission.
- `approve(alpha_id)` marks an alpha as approved.
- `veto(alpha_id, reason)` marks an alpha as vetoed; veto blocks allocations.
- `override_veto(alpha_id, reason)` allows a governance override (must be logged).
- All actions are appended to `audit_log` with timestamp, actor and reason.

API
- `Governance` class with methods: `submit_for_approval`, `approve`, `veto`, `override_veto`, `is_approved`, `is_vetoed`, `get_audit`.

Notes
- Governance may veto but should not micromanage allocation sizing. For enforcement, pipelines should check `is_vetoed` before allocating capital.
