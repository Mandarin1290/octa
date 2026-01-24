from datetime import datetime, timedelta, timezone

from octa_ops.data_failures import DataFeedManager


def test_data_freshness_and_degradation():
    now = datetime(2025, 12, 28, 12, 0, 0, tzinfo=timezone.utc)

    def now_fn():
        return now

    mgr = DataFeedManager(freshness_seconds=5, recovery_required=2, now_fn=now_fn)
    mgr.set_hierarchy("ABC", ["primary", "fallback"])

    # no updates => no fresh feeds
    allowed, reason = mgr.allow_trade("ABC", "entry")
    assert not allowed and reason == "no_fresh_feed"

    # report primary update -> fresh
    mgr.report_update("primary", "ABC", ts=now)
    assert mgr.is_fresh_feed("primary")
    allowed, reason = mgr.allow_trade("ABC", "entry")
    assert allowed and reason == "primary_fresh"

    # advance time so primary stale but fallback fresh
    now2 = now + timedelta(seconds=10)

    def now_fn2():
        return now2

    mgr.now_fn = now_fn2
    # update fallback at now2 (fresh)
    mgr.report_update("fallback", "ABC", ts=now2)
    assert not mgr.is_fresh_feed("primary")
    assert mgr.is_fresh_feed("fallback")
    assert mgr.is_degraded("ABC")
    # entries blocked, exits allowed
    allowed_e, r_e = mgr.allow_trade("ABC", "entry")
    allowed_x, r_x = mgr.allow_trade("ABC", "exit")
    assert not allowed_e and r_e == "degraded_blocks_entries"
    assert allowed_x and r_x == "degraded_exit_allowed"
