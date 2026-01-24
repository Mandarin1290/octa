from __future__ import annotations

from datetime import datetime
from typing import Callable


def run_data_integrity_failure(
    check_data_fn: Callable[[], bool],
    incident_recorder: Callable[[dict], None],
    sentinel_api,
):
    ts = datetime.utcnow().isoformat() + "Z"
    ok = check_data_fn()
    if not ok:
        incident = {
            "ts": ts,
            "type": "data_integrity_failure",
            "detail": "checksum_mismatch",
        }
        incident_recorder(incident)
        try:
            sentinel_api.set_gate(3, "data_integrity_failure")
        except Exception:
            pass
        return {"pass": False, "incident": incident}
    return {"pass": True}
