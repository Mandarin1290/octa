from __future__ import annotations

from datetime import datetime
from typing import Callable


def run_message_bus_backlog(
    get_backlog_size: Callable[[], int],
    threshold: int,
    incident_recorder: Callable[[dict], None],
    sentinel_api,
):
    ts = datetime.utcnow().isoformat() + "Z"
    size = get_backlog_size()
    if size >= threshold:
        incident = {"ts": ts, "type": "message_bus_backlog", "backlog": size}
        incident_recorder(incident)
        try:
            sentinel_api.set_gate(2, "message_bus_backlog")
        except Exception:
            pass
        return {"pass": False, "incident": incident}
    return {"pass": True}
