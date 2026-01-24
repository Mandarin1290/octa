from __future__ import annotations

from datetime import datetime
from typing import Callable


def run_execution_ack_timeout(
    get_timeout_count: Callable[[], int],
    threshold: int,
    incident_recorder: Callable[[dict], None],
    sentinel_api,
):
    ts = datetime.utcnow().isoformat() + "Z"
    count = get_timeout_count()
    if count >= threshold:
        incident = {"ts": ts, "type": "execution_ack_timeout_storm", "count": count}
        incident_recorder(incident)
        try:
            sentinel_api.set_gate(3, "execution_ack_timeout")
        except Exception:
            pass
        return {"pass": False, "incident": incident}
    return {"pass": True}
