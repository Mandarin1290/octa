#!/usr/bin/env python3
"""Simple local demo runner for Nexus in paper mode.

Starts supervisor with a heartbeat publisher, a signal publisher and a simple consumer.
"""

from __future__ import annotations

import time
from multiprocessing import Process

from octa_nexus.bus import NexusBus
from octa_nexus.messages import Healthbeat, SignalEvent, _now_iso


def heartbeat_publisher(bus_path: str, name: str, interval: float = 1.0):
    b = NexusBus(bus_path)
    while True:
        hb = Healthbeat(
            id=f"hb-{name}-{int(time.time())}",
            type="Healthbeat",
            ts=_now_iso(),
            component=name,
            epoch=_now_iso(),
        )
        b.publish(hb)
        time.sleep(interval)


def signal_publisher(bus_path: str):
    b = NexusBus(bus_path)
    cnt = 0
    while True:
        cnt += 1
        se = SignalEvent(
            id=f"sig-{cnt}",
            type="SignalEvent",
            ts=_now_iso(),
            model="m1",
            symbol="EURUSD",
            score=0.5,
        )
        b.publish(se)
        time.sleep(2.0)


def simple_consumer(bus_path: str):
    b = NexusBus(bus_path)
    while True:
        msg = b.claim("simple_consumer", timeout=1.0)
        if not msg:
            time.sleep(0.5)
            continue
        print("consumed", msg)
        # ack
        b.ack(msg.id)


def main():
    import os

    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "_nexus_bus"))
    p1 = Process(target=heartbeat_publisher, args=(path, "training"))
    p2 = Process(target=heartbeat_publisher, args=(path, "execution"))
    p3 = Process(target=signal_publisher, args=(path,))
    p4 = Process(target=simple_consumer, args=(path,))
    for p in (p1, p2, p3, p4):
        p.start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        for p in (p1, p2, p3, p4):
            p.terminate()


if __name__ == "__main__":
    main()
