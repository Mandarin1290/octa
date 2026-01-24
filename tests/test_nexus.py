import os
import sqlite3
import time
from multiprocessing import Process

from octa_nexus import bus as nexus_bus
from octa_nexus import messages
from octa_nexus.health import HealthMonitor


def test_message_persistence(tmp_path):
    p = tmp_path / "bus"
    b = nexus_bus.NexusBus(str(p))
    msg = messages.OrderIntent(
        id="m1",
        type="OrderIntent",
        ts=messages._now_iso(),
        order_id="o1",
        symbol="X",
        side="BUY",
        qty=1.0,
        price=100.0,
    )
    b.publish(msg)
    # reopen bus
    b2 = nexus_bus.NexusBus(str(p))
    rows = b2.list_undelivered()
    assert any(r["id"] == "m1" for r in rows)


def consumer_worker(bus_path: str, consumer_db: str, crash_first: bool):
    b = nexus_bus.NexusBus(bus_path)
    proc = b.claim("c1", timeout=0.1)
    if not proc:
        return
    mid = proc.id
    # local processed store
    conn = sqlite3.connect(consumer_db)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS processed(id TEXT PRIMARY KEY)")
    cur.execute("SELECT 1 FROM processed WHERE id=?", (mid,))
    if cur.fetchone():
        # already processed, just ack
        b.ack(mid)
        return
    # mark processed
    cur.execute("INSERT OR IGNORE INTO processed(id) VALUES(?)", (mid,))
    conn.commit()
    if crash_first:
        os._exit(1)
    # ack
    b.ack(mid)


def test_consumer_idempotency(tmp_path):
    p = tmp_path / "bus"
    dbpath = str(p)
    b = nexus_bus.NexusBus(dbpath)
    msg = messages.OrderIntent(
        id="m2",
        type="OrderIntent",
        ts=messages._now_iso(),
        order_id="o2",
        symbol="Y",
        side="SELL",
        qty=2.0,
        price=50.0,
    )
    b.publish(msg)

    consumer_db = str(tmp_path / "consumer.db")

    # first run crashes after marking processed (no ack)
    p1 = Process(target=consumer_worker, args=(dbpath, consumer_db, True))
    p1.start()
    p1.join()
    # allow lock expiry
    time.sleep(0.2)
    # second run should see processed flag and ack without reprocessing
    p2 = Process(target=consumer_worker, args=(dbpath, consumer_db, False))
    p2.start()
    p2.join()

    # bus should have no undelivered messages
    b2 = nexus_bus.NexusBus(dbpath)
    assert not any(True for _ in b2.list_undelivered())

    # processed table should have one entry
    conn = sqlite3.connect(consumer_db)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM processed")
    cnt = cur.fetchone()[0]
    assert cnt == 1


def test_heartbeat_missing_triggers_freeze(tmp_path):
    p = tmp_path / "bus"
    b = nexus_bus.NexusBus(str(p))
    # publish an old heartbeat
    old_ts = "2000-01-01T00:00:00+00:00"
    hb = messages.Healthbeat(
        id="hb-old", type="Healthbeat", ts=old_ts, component="worker1", epoch=old_ts
    )
    b.publish(hb)

    events = []

    def on_fail(comp: str):
        events.append(comp)

    hm = HealthMonitor(str(p), check_interval=0.5, expiry=1.0, on_failure=on_fail)
    hm.start()
    # wait for monitor to detect
    time.sleep(2.0)
    hm.stop()

    # check that on_failure was called for worker1
    assert "worker1" in events
