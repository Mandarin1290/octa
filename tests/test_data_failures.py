from datetime import datetime, timedelta, timezone

from octa_ops.data_failures import DataFeedManager


class FixedClock:
    def __init__(self, now: datetime):
        self._now = now

    def now(self):
        return self._now

    def advance(self, seconds: int):
        self._now = self._now + timedelta(seconds=seconds)


def test_stale_data_blocks_trading():
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    clock = FixedClock(start)
    dfm = DataFeedManager(freshness_seconds=5, recovery_required=2, now_fn=clock.now)
    dfm.set_hierarchy("BTC", ["p", "b"])
    # report on primary at t=0
    dfm.report_update("p", "BTC", ts=clock.now())
    # advance beyond freshness
    clock.advance(10)
    allowed, reason = dfm.allow_trade("BTC", trade_type="entry")
    assert not allowed
    assert reason in ("no_fresh_feed", "degraded_blocks_entries")


def test_recovery_verified():
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    clock = FixedClock(start)
    dfm = DataFeedManager(freshness_seconds=5, recovery_required=2, now_fn=clock.now)
    dfm.set_hierarchy("ETH", ["p", "b"])
    # primary reports fresh at t=0
    dfm.report_update("p", "ETH", ts=clock.now())
    # advance and report again -> consecutive fresh
    clock.advance(1)
    dfm.report_update("p", "ETH", ts=clock.now())
    assert dfm.recovered("ETH") is True
    allowed, reason = dfm.allow_trade("ETH", trade_type="entry")
    assert allowed
