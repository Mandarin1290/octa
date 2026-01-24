from __future__ import annotations

import time
from typing import Any

from octa_nexus.bus import NexusBus
from octa_nexus.messages import Incident, OrderIntent, RiskDecision, _now_iso


def run(bus_path: str, tmpdir) -> dict[str, Any]:
    b = NexusBus(bus_path)
    # publish an order
    o = OrderIntent(
        id="ord-timeout",
        type="OrderIntent",
        ts=_now_iso(),
        order_id="ord-timeout",
        symbol="X",
        side="BUY",
        qty=1.0,
        price=1.0,
    )
    b.publish(o)

    # consumer claims but doesn't ack (simulate by calling claim and not acking)
    b.claim("slow_consumer", timeout=0.1)

    # watcher: after threshold detect stale lock and publish Incident + RiskDecision
    time.sleep(0.3)
    rows = b.list_undelivered()
    incidents = 0
    freezes = 0
    for r in rows:
        if r["locked_by"]:
            # simulate detection
            inc = Incident(
                id="inc-1",
                type="Incident",
                ts=_now_iso(),
                component="consumer",
                severity="HIGH",
                msg="ack_timeout",
            )
            rd = RiskDecision(
                id="rd-ack",
                type="RiskDecision",
                ts=_now_iso(),
                decision="FREEZE",
                reason="ack_timeout",
            )
            b.publish(inc)
            b.publish(rd)
            incidents += 1
            freezes += 1

    return {"incidents": incidents, "freezes": freezes}
