from __future__ import annotations

import time
from typing import Any

from octa_nexus.bus import NexusBus
from octa_nexus.health import HealthMonitor


def run(bus_path: str, tmpdir) -> dict[str, Any]:
    NexusBus(bus_path)
    # do not publish heartbeats; start monitor
    events = []

    def on_fail(comp: str):
        events.append(comp)

    hm = HealthMonitor(bus_path, check_interval=0.2, expiry=0.5, on_failure=on_fail)
    hm.start()
    time.sleep(1.0)
    hm.stop()
    return {"frozen_components": list(events)}
