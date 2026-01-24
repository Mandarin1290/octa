# Continuous Audit & Compliance Readiness

Purpose
-------
Maintain perpetual audit readiness with rolling snapshots, attestations and control effectiveness logs.

Hard Rules
----------
- Audit is continuous: snapshots and attestations are produced and appended to the audit log.
- Evidence immutable: snapshots and attestations include canonical evidence hashes for integrity verification.

Features
--------
- `take_snapshot(name, components)` — capture a rolling snapshot and return `snapshot_id`.
- `verify_snapshot(snapshot_id)` — recompute canonical hash and detect tampering.
- `attest_compliance(name, attestor, statement)` — record attestation with evidence hash.
- `record_control_effectiveness(control_id, status, notes)` — log control checks.

Notes
-----
- Evidence hashes use canonical JSON (sorted keys, compact separators) and SHA‑256.
- Store snapshots and audit logs in append-only storage for production; here they are in-memory for tests.
