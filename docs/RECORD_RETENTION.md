# Record Keeping & Retention Policy Enforcer

Purpose: enforce retention periods, prevent premature deletion, and provide secure deletion workflows that are auditable.

Hard rules:

- Records are immutable until their retention period expires.
- Deletion before expiry is blocked and recorded in the audit log.
- All deletions and purges are logged with timestamp and justification.

API (see `octa_reg.record_retention`):

- `create_record(classification, payload, retention_days, actor)` — create a classified record.
- `attempt_delete(record_id, justification, actor)` — attempt secure deletion; raises if retention not expired.
- `purge_expired()` — permanently remove records that were marked deleted and whose retention has expired.
- `list_records()` — deterministic listing of current records.

Implementation notes:

- Use a testable `now_fn` when constructing `RetentionManager` to simulate passage of time in tests.
- Persist audit logs to append-only storage for compliance.
