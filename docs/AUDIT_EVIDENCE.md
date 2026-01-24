# Audit Evidence Generation

Purpose: generate structured, immutable, and reproducible evidence for audits that links to control objectives.

Key rules:

- Evidence must be immutable: store snapshots and hashes; any modification is detectable.
- Evidence must be reproducible from system state: hash is computed from a canonical serialization of the snapshot and control ids (timestamp excluded).
- No manual screenshots or ad-hoc files — use structured JSON evidence.

API (see `octa_reg.audit_evidence`):

- `create_evidence(snapshot: dict, control_ids: List[str]) -> Evidence` — produce evidence with deterministically computed SHA‑256 hash.
- `verify_evidence(evidence: Evidence) -> bool` — recompute hash and validate integrity.
- `export_evidence_json(evidence)` / `load_evidence_json(json_str)` — serialize/deserialize evidence for archival.

Implementation notes:

- Canonical JSON serialization with sorted keys is used for determinism.
- Hash covers only `snapshot` and `control_ids` so that evidence is reproducible from the same system state; `ts` and `id` are metadata only.
- Persist exported JSON to an append-only store (S3 with versioning, signed ledger) for long-term immutability.
