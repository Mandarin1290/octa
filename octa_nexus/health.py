from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from typing import Callable

from .bus import NexusBus
from .messages import RiskDecision, _now_iso


class HealthMonitor:
    def __init__(
        self,
        bus_path: str,
        check_interval: float = 2.0,
        expiry: float = 5.0,
        on_failure: Callable[[str], None] | None = None,
    ):
        self.bus = NexusBus(bus_path)
        self.interval = check_interval
        self.expiry = expiry
        self._stop = False
        self.on_failure = on_failure

    def start(self) -> None:
        t = threading.Thread(target=self.run, daemon=True)
        t.start()

    def stop(self) -> None:
        self._stop = True

    def run(self) -> None:
        while not self._stop:
            now = time.time()
            rows = self.bus.recent_by_type("Healthbeat", limit=200)
            # check latest heartbeat per component
            last_hb: dict[str, float] = {}
            for r in rows:
                try:
                    payload = json.loads(r["payload"])
                except Exception:
                    continue
                comp = payload.get("component")
                ts = payload.get("ts")
                if comp and ts:
                    try:
                        t = datetime.fromisoformat(ts).timestamp()
                        last_hb[comp] = max(last_hb.get(comp, 0.0), t)
                    except Exception:
                        continue

            # find missing heartbeats
            for comp, t in list(last_hb.items()):
                if now - t > self.expiry:
                    # publish RiskDecision freeze
                    dec = RiskDecision(
                        id=f"rd-{comp}-{int(now)}",
                        type="RiskDecision",
                        ts=_now_iso(),
                        decision="FREEZE",
                        reason=f"missing_heartbeat:{comp}",
                    )
                    self.bus.publish(dec)
                    if self.on_failure:
                        try:
                            self.on_failure(comp)
                        except Exception:
                            pass
            time.sleep(self.interval)
