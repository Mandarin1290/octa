# Operator Access & Action Model

Overview
--------
Operators are human users with constrained roles. They cannot trade. They can only invoke predefined actions through a controlled CLI. Every action is audited and permission-checked. Dangerous actions require two operator confirmations (dual-control).

Roles
-----
- `VIEW` — read-only, can view status and logs.
- `INCIDENT` — can acknowledge/incidents and run incident-level runbooks.
- `EMERGENCY` — allowed to perform emergency actions; dangerous actions require dual-control.

CLI
---
Use `octa_ops.cli.OperatorCLI` with registered `OperatorRegistry` and `ActionRegistry`. For dangerous actions, provide `signature` and `signature2` and set `ctx["second_operator"]`.

Security
--------
- Operator keys are shared secrets used to produce simple HMAC-like signatures for dual-control. In production, replace with proper cryptographic signatures.
- All actions and signature verifications are audited via `audit_fn`.
