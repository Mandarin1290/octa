Wargame Framework

Overview

The war-game framework provides deterministic, auditable simulation runs used for preparedness exercises and stress-testing governance, ops and regulatory responses.

Principles

- Isolation: each scenario receives a deep-copied context so simulations cannot mutate production state.
- Determinism: scenarios execute with a provided RNG seed and produce canonical outputs.
- Auditability: each run records timestamp, seed, deterministic hash of (scenario,seed,output), and context id.
- Replayability: recorded runs can be replayed against fresh contexts to validate reproducibility.

Usage

- Register scenarios using `WarGameFramework.register_scenario(name, fn)` where `fn(payload, rng)`.
- Run with `run_scenario(name, context_payload, seed=None, metadata=None)`.
- Replay with `replay_result(result, context_payload)`.
- Export result JSON using `export_result_json(result)`.

Integration

- Results can be hashed and stored using the audit evidence utilities in `octa_reg.audit_evidence` for immutable retention.
- Use `metadata` to include operator, intent, and classification information for governance records.
