from datetime import datetime, timedelta, timezone

from octa_stream.live_feed import Bar, LiveFeed
from octa_stream.live_quality import LiveQualityChecker


class SentinelMock:
    def __init__(self):
        self.calls = []

    def set_gate(self, level, reason):
        self.calls.append((level, reason))


def test_stale_feed_blocked(tmp_path):
    sentinel = SentinelMock()
    checker = LiveQualityChecker(max_latency_seconds=1)
    feed = LiveFeed(quality_checker=checker, sentinel_api=sentinel)

    old_ts = datetime.now(timezone.utc) - timedelta(seconds=10)
    b = Bar(instrument="AAPL", timestamp=old_ts, open=100, high=101, low=99, close=100)
    ok = feed.on_bar_receive(b)
    assert ok is False
    assert any(r[1].startswith("stale") for r in sentinel.calls)


def test_bad_timestamps_rejected():
    sentinel = SentinelMock()
    checker = LiveQualityChecker(max_latency_seconds=60)
    feed = LiveFeed(quality_checker=checker, sentinel_api=sentinel)

    # first bar timezone-aware
    ts1 = datetime.now(timezone.utc)
    b1 = Bar(instrument="FUT_ES", timestamp=ts1, open=1, high=2, low=0.5, close=1)
    assert feed.on_bar_receive(b1) is True

    # second bar naive (mixing kinds) -> rejected
    ts2 = datetime.now() + timedelta(seconds=1)
    b2 = Bar(instrument="FUT_ES", timestamp=ts2, open=1, high=2, low=0.5, close=1)
    ok = feed.on_bar_receive(b2)
    assert ok is False
    assert any(r[1].startswith("bad_timestamp_kind") for r in sentinel.calls)
