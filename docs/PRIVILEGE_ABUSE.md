Privilege Abuse Simulation

Purpose

Simulate insider abuse and privilege escalation attempts and ensure the system enforces least privilege, logs attempts, and preserves survivability without implicit trust.

Scenarios

- Unauthorized config change: user without role attempts to change sensitive config.
- Bypass risk gates: attempt to skip or override automated risk checks.
- Illicit capital reallocation: move capital between accounts without permission.

Design

- `PrivilegeManager` holds simple role->action policies and records detailed audit entries for any attempt (allowed or denied).
- `ConfigStore` models a minimal configuration/ledger and keeps an audit trail for changes.
- `User` objects carry role lists; enforcement is via `check_permission` and guarded action methods.

Hard rules

- Least privilege is enforced by `PrivilegeManager` policies.
- All abuse attempts are logged; allowed actions also create audit entries.
- System operations do not implicitly trust users; operations require explicit role privileges.

Usage

- Create `ConfigStore()` and `PrivilegeManager(config)`.
- Use `User(id, roles)` to represent callers and call `attempt_change_config`, `attempt_bypass_risk`, or `attempt_reallocate`.
- Inspect `PrivilegeManager.audit` and `ConfigStore.audit` for a complete trail.
