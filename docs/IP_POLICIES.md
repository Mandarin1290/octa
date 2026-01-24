# IP Policies (Enforced at Runtime)

This document describes the runtime-enforced IP and license policies for OCTA.

Key principles
- Policies are code-enforced. Violations halt execution and are auditable.
- Rules are intentionally minimal and expressed programmatically in `PolicyEnforcer`.

Categories and policy summary

- CORE_PROPRIETARY
  - Must not depend on `OPEN_SOURCE_DERIVED` modules (to avoid relicensing/exposure).

- LICENSABLE
  - Can depend on open-source; license compatibility must be managed outside the runtime
    (but runtime checks for obvious escalations can be added).

- INTERNAL_ONLY
  - Must not be imported by modules owned by other teams or owners. Runtime checks
    compare `ModuleMap` owners to detect external use.

- OPEN_SOURCE_DERIVED
  - Must not depend on `CORE_PROPRIETARY` modules (would create a non-free distribution).

Enforcement
- `octa_ip.policy_enforcer.PolicyEnforcer.enforce(module_map, classifier)` runs checks
  and appends structured entries to `PolicyEnforcer.audit_log`.
- Any violation raises `RuntimeError` and thus halts execution (hard rule).

Audit log
- Each audit entry contains: `ts` (UTC ISO), `level` (info/error), `msg`, and `meta`.

Extending policies
- Add rules to `PolicyEnforcer.policy_rules()`; expand checks in `check_policies()` for
  additional constraints (license strings, SPDX tags, binary artifacts, etc.).

CI integration
- Recommended: run the `PolicyEnforcer` as part of CI (pre-merge) using a small runner
  that builds a `ModuleMap`, instantiates the `IPClassifier`, and calls `enforce()`.
