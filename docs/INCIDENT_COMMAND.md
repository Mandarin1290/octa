# Incident Command & Escalation

This module implements centralized incident command logic with strict rules:

- Exactly one command authority per incident is enforced.
- Escalation path is predefined by `IncidentManager.ESCALATION_RULES`.
- Manual overrides are auditable and appended to command state.

Key API
- `CommandManager.start_command(incident_id, initial_timeout)` — create command state.
- `CommandManager.assign_commander(incident_id, commander, role, actor, reason)` — assign commander; raises if another commander exists.
- `CommandManager.override_commander(...)` — perform manual override; always recorded in audits.
- `CommandManager.check_escalations()` — perform timeout-based escalations; supports injected time provider for deterministic tests.
