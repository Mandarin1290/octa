**Operational Runbooks**

Kurzbeschreibung
- `octa_ops.runbooks` stellt strukturierte, maschinenlesbare Runbooks bereit, die ohne menschliche Interpretation ausgeführt oder simuliert werden können.
- Jede Runbook enthält: `incident_type`, `summary`, `immediate_actions`, `escalation_chain`, `recovery_steps`.

Formatregeln
- `immediate_actions` und `recovery_steps` sind Listen von Schritten. Jeder Schritt ist ein Dict mit mindestens `action_type`.
- Supported `action_type` Beispiele:
  - `command`: erfordert `command` und `expected_result` (maschinenprüfbar)
  - `check`: prüft einen Zustand, erfordert `check_id` und `expected_value`
  - `notify`: sendet Nachricht an `target`

Audit
- Alle Änderungen und Ausführungen werden in `RunbookManager.audit_log` protokolliert (append-only) mit `ts`, `actor`, `action`, `details`.

Beispiel
```py
from octa_ops.runbooks import Runbook, RunbookManager

rb = Runbook(
    incident_type="data_feed_stale",
    summary="Price feed stale beyond acceptable window",
    immediate_actions=[
        {"action_type": "command", "command": "restart_data_feed", "expected_result": "service_running"},
        {"action_type": "check", "check_id": "last_update_age_s", "expected_value": "<10"},
    ],
    escalation_chain=[{"role": "ops", "contact": "ops@company"}, {"role": "risk", "contact": "risk@company"}],
    recovery_steps=[
        {"action_type": "command", "command": "switch_to_fallback_feed", "expected_result": "fallback_active"}
    ],
)

mgr = RunbookManager()
mgr.add_runbook(rb, actor="onboard_script")
mgr.execute_runbook("data_feed_stale", actor="ops_user", simulate=True)
```
# Runbooks (Executable Operational Procedures)

Overview
--------
Runbooks are implemented as executable sequences of steps (functions). Each step returns a `StepResult` and every step execution is audited. Failures escalate incidents and notify the Sentinel.

Usage
-----
Use `RunbookEngine` to register runbooks and execute them with a context dict. Provide `audit_fn` and `sentinel_api` when creating the engine.

Example
-------
1. Register steps from `octa_ops.library.*` modules.
2. Call `engine.execute("broker_disconnect", ctx)` to run the broker disconnect runbook.

Files
-----
- `octa_ops/runbooks.py` — runbook engine and `StepResult`.
- `octa_ops/library/*` — provided runbook steps for broker, datafeed, correlation, drawdown, and kill activation.
