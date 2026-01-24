Audit Trail — Threat Model and Guarantees

Goals

- Provide an append-only, tamper-evident, auditable event trail for critical platform events.
- Ensure cryptographic integrity (hash chaining) and batch signing (ed25519) to detect tampering.
- Fail closed: if logging cannot be performed or verified, trading must be blocked.

Design

- Events are JSON records written newline-delimited to a local append-only file `ledger.log`.
- Each event contains `prev_hash` and `curr_hash` (SHA256 over canonical JSON). Chaining enforces order integrity.
- An SQLite index (`ledger.db`) stores offsets for fast queries (`last_n`, `by_time_range`, `by_action`).
- Batch signing (ed25519) signs sequences of N events to provide non-repudiation for batches. Keys are managed by `octa_fabric` secrets backend.

Threat Model

- Insider tampering: attacker with write access to `ledger.log` can attempt to alter events; hash-chain verification will detect inconsistencies unless the attacker can also forge signatures and recompute hashes and signatures for all downstream events and signatures (requires private key).
- Host compromise: if the host is fully compromised (attacker has private signing key and can modify log and DB), guarantees are reduced; use separated key management and protected hardware for higher assurance.

Operational Notes

- Keep signing private key out of plaintext repository; use `EncryptedFileBackend` or system keyring.
- Backup ledger.log and ledger.db and maintain WORM storage if needed.
- On any verification failure, systems should initiate incident workflow and block execution flows.

Limitations

- Local file store is a single-instance store. For multi-host setups, consider pushing events to an append-only replicated store.
- Current implementation uses ed25519 raw keys; in production use a KMS/HSM.
