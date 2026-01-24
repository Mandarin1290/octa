from datetime import datetime, timedelta, timezone

from octa_wargames.data_poisoning import (
    DataFeed,
    DataPoisoningSimulator,
    DetectionEngine,
    MarketExecutionGuard,
)


def make_feed(name="p", price=100.0, now=None):
    if now is None:
        now = datetime.now(timezone.utc)
    ts = now.isoformat()
    f = DataFeed(name=name, price=price, ts=ts)
    f.history.append({"ts": ts, "price": price})
    return f


def test_corrupted_feed_blocked():
    now = datetime.now(timezone.utc)
    primary = make_feed("primary", 100.0, now=now)
    backup = make_feed("backup", 101.0, now=now)

    # inject a spike into primary
    DataPoisoningSimulator.price_spike(primary, 0.5)

    detector = DetectionEngine(max_age_seconds=10, spike_threshold=0.2)
    guard = MarketExecutionGuard(detector)

    is_corr, reason = guard.validate_feed(primary)
    assert is_corr is True
    assert reason == "price_spike"

    chosen, r = guard.select_feed(primary, [backup])
    assert chosen.name == "backup"
    assert r == "price_spike"


def test_fallback_activated_for_delayed_timestamp():
    now = datetime.now(timezone.utc)
    primary = make_feed("primary", 100.0, now=now - timedelta(seconds=120))
    backup = make_feed("backup", 100.5, now=now)

    # primary is delayed
    detector = DetectionEngine(max_age_seconds=30)
    guard = MarketExecutionGuard(detector)

    is_corr, reason = guard.validate_feed(primary)
    assert is_corr is True
    assert reason == "delayed_timestamp"

    chosen, r = guard.select_feed(primary, [backup])
    assert chosen.name == "backup"
    assert r == "delayed_timestamp"
