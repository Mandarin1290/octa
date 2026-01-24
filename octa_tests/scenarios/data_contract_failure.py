from __future__ import annotations

import time
from typing import Any

from octa_nexus.bus import NexusBus
from octa_nexus.messages import OrderIntent, RiskDecision, SignalEvent, _now_iso


def run(bus_path: str, tmpdir) -> dict[str, Any]:
    b = NexusBus(bus_path)
    # publish a signal for a BAD asset
    sig = SignalEvent(
        id="sig-bad",
        type="SignalEvent",
        ts=_now_iso(),
        model="m1",
        symbol="BAD_ASSET",
        score=0.9,
    )
    b.publish(sig)

    # data validator consumes and decides asset ineligible
    # simulate: read signal and publish RiskDecision ineligible
    rows = b.list_undelivered()
    for r in rows:
        if r["type"] == "SignalEvent":
            rd = RiskDecision(
                id="rd-bad",
                type="RiskDecision",
                ts=_now_iso(),
                decision="INELIGIBLE",
                reason="schema_mismatch",
            )
            b.publish(rd)

    # Portfolio manager checks for risk decisions before publishing orders
    # Attempt to publish an OrderIntent but check recent RiskDecision
    rds = b.recent_by_type("RiskDecision", limit=50)
    blocked = any("INELIGIBLE" in r["payload"] for r in rds)
    if not blocked:
        ord = OrderIntent(
            id="o1",
            type="OrderIntent",
            ts=_now_iso(),
            order_id="o1",
            symbol="BAD_ASSET",
            side="BUY",
            qty=1.0,
            price=1.0,
        )
        b.publish(ord)

    time.sleep(0.1)
    # results
    od = [r for r in b.list_undelivered() if r["type"] == "OrderIntent"]
    return {"orders_published": len(od), "blocked": blocked}
