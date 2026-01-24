from datetime import datetime, timedelta

from octa_reg.surveillance import SurveillanceEngine


def iso(t: datetime) -> str:
    return t.isoformat() + "Z"


def test_suspicious_spoofing_flagged():
    se = SurveillanceEngine(
        window_seconds=60, small_order_size=5.0, spoofing_cancel_ratio=0.6
    )
    base = datetime.utcnow()
    actor = "trader_x"
    inst = "SYM"

    # create several small new orders then cancels within window
    for i in range(4):
        se.ingest(
            {
                "id": f"o{i}",
                "actor": actor,
                "instrument": inst,
                "side": "buy",
                "price": 100 + i,
                "qty": 1.0,
                "type": "new",
                "ts": iso(base + timedelta(seconds=i)),
            }
        )
    # now cancels
    for i in range(4):
        se.ingest(
            {
                "id": f"c{i}",
                "actor": actor,
                "instrument": inst,
                "side": "buy",
                "price": 100 + i,
                "qty": 1.0,
                "type": "cancel",
                "ts": iso(base + timedelta(seconds=10 + i)),
            }
        )

    # there should be at least one spoofing_like alert
    patterns = [a.pattern for a in se.alert_log]
    assert "spoofing_like" in patterns


def test_benign_behavior_ignored():
    se = SurveillanceEngine(window_seconds=60)
    base = datetime.utcnow()
    # larger orders, few cancels
    se.ingest(
        {
            "id": "o1",
            "actor": "t1",
            "instrument": "SYM",
            "side": "buy",
            "price": 100,
            "qty": 100.0,
            "type": "new",
            "ts": base.timestamp(),
        }
    )
    se.ingest(
        {
            "id": "o2",
            "actor": "t1",
            "instrument": "SYM",
            "side": "buy",
            "price": 101,
            "qty": 50.0,
            "type": "new",
            "ts": base.timestamp() + 1,
        }
    )
    se.ingest(
        {
            "id": "f1",
            "actor": "t1",
            "instrument": "SYM",
            "side": "buy",
            "price": 100,
            "qty": 100.0,
            "type": "fill",
            "ts": base.timestamp() + 2,
        }
    )

    # no alerts for benign large fills
    assert len(se.alert_log) == 0
