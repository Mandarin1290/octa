# Disaster Recovery & Position Reconciliation

Purpose: ensure internal position state exactly matches broker state after incidents, prevent phantom exposures and block trading until recovery completes.

Key rules:
- Positions must reconcile exactly — any mismatch triggers recovery.
- No phantom exposure allowed — internal exposures are set to broker snapshot during resolution.
- Recovery blocks automated trading until explicitly completed.

Workflow:
1. Create a checkpoint of internal state.
2. Reconcile internal positions with broker snapshot.
3. If mismatches are detected, enter recovery mode (trading blocked).
4. Optionally auto-resolve by aligning to broker snapshot (requires broker data validation).
5. After successful resolution and verification, mark recovery complete and resume trading.
6. If resolution fails, keep trading blocked and escalate to operators.

Implementation:
- See `octa_ops/recovery.py` for `RecoveryManager`.
- Use `create_checkpoint` before any reconciliation so you can restore previous state if needed.

Operator guidance:
- Prefer manual review if broker snapshots are ambiguous.
- Persist checkpoints and audit logs to an append-only store for compliance.
