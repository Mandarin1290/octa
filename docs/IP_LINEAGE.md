# IP Audit & Lineage Tracker

This document describes the lineage tracker used to record, verify and export immutable
change history for code modules.

Concepts
- Each change to a module is recorded as a `LineageEntry` containing:
  - `module`, `ts` (UTC ISO), `author`, `description`, `content_hash`, `prev_hash`, `entry_hash`.
- `content_hash` is the SHA-256 of the changed content (only the hash is stored).
- `entry_hash` is computed as SHA-256 over the concatenation of `prev_hash|module|ts|author|description|content_hash`.
- Chains are immutable by design; any mutation will break the hash chain and be detected during verification.

Usage
- Use `LineageTracker.add_change(module, author, description, content)` to record a change.
- Use `LineageTracker.verify_module(module)` to assert the chain is untampered (raises on tampering).
- Export a module's chain with `LineageTracker.export_chain(module)` for due diligence and valuation.

Security
- The tracker stores only hashes of content; keep actual content in version control or secure storage.
- For production, persist the chain in an append-only audit store (WORM storage, signed ledger, or commit to an immutable backing store).
