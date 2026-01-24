# Change Management & Release Governance

Purpose: control system changes, enforce approvals, track emergency changes and link rollback plans.

Hard rules:

- No production change without approval (unless emergency override).
- Emergency changes are tracked separately and must include justification.
- Rollback plans are required for approvals and must be linked to the change request.

API (see `octa_reg.change_mgmt`):

- `create_request(title, description, proposer)` — create a change request.
- `approve_request(request_id, approver, rollback_plan, release_tag)` — approve with rollback plan.
- `emergency_override(request_id, actor, justification)` — allow emergency application and log justification.
- `tag_release(request_id, tag, actor)` — add a release tag.
- `link_rollback(request_id, rollback_plan, actor)` — attach or update rollback plan.
- `apply_change(request_id, actor)` — enforce approvals and apply; blocked if not approved unless emergency override.

Operator guidance:

- Always include a rollback plan before approving changes.
- Log emergency overrides and ensure follow-up retrospective approvals.
