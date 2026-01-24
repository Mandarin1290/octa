# Alpha Pipeline Audit Trail

This module provides an append-only, hash-linked audit trail for alpha
generation. It guarantees:

- Hash-based lineage: `compute_lineage_hash(hypothesis_meta, data_snapshot)`
  returns a deterministic SHA-256 hash tying the alpha to the hypothesis and the
  exact data snapshot used to generate signals.
- Pipeline step logs: each pipeline step can `append(event, payload)` to an
  `AuditChain`, creating immutable `AuditBlock`s with linked hashes.
- Immutable records: `AuditBlock` is frozen and the chain's `verify()` method
  detects tampering by validating block hashes.

Usage
-----
Create an `AuditChain`, write step logs during pipeline execution, and store
the lineage hash in the chain as the canonical identifier for the run.
