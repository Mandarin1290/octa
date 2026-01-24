from __future__ import annotations

from datetime import datetime
from typing import Callable


def run_audit_degraded(
    audit_fn: Callable[[str, dict], None],
    incident_recorder: Callable[[dict], None],
    sentinel_api,
    simulate_failure: bool = True,
):
    ts = datetime.utcnow().isoformat() + "Z"
    try:
        if simulate_failure:
            # attempt to write; audit_fn may raise
            audit_fn("drill.audit.check", {"ts": ts, "status": "start"})
            raise IOError("simulated slow/failure")
        else:
            audit_fn("drill.audit.check", {"ts": ts, "status": "ok"})
            return {"pass": True}
    except Exception as e:
        incident = {"ts": ts, "type": "audit_degraded", "error": str(e)}
        incident_recorder(incident)
        # fail-closed
        try:
            sentinel_api.set_gate(3, "audit_degraded")
        except Exception:
            pass
        return {"pass": False, "incident": incident}
