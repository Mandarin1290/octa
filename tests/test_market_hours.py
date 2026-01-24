from datetime import date, datetime, time, timezone

from octa_core.calendar import Session, VenueCalendar, registry
from octa_vertex.market_hours import pretrade_check


def test_timezone_open_close():
    # NYSE is registered; check 09:35 ET == 14:35 UTC (avoid open-auction do-not-trade window)
    ts_utc = datetime(2025, 12, 15, 14, 35, tzinfo=timezone.utc)
    res = pretrade_check({"venue": "NYSE"}, ts=ts_utc)
    assert res["eligible"] is True

    # 09:00 ET -> 14:00 UTC should be closed
    ts_utc2 = datetime(2025, 12, 15, 14, 0, tzinfo=timezone.utc)
    res2 = pretrade_check({"venue": "NYSE"}, ts=ts_utc2)
    assert res2["eligible"] is False


def test_holiday_closes_market(tmp_path):
    # create a temporary calendar with a holiday
    cal = VenueCalendar(
        "TESTEX",
        "UTC",
        sessions=[Session(0, time(9, 0), time(17, 0))],
        holidays=[date(2025, 12, 25)],
    )
    registry.register(cal)
    ts = datetime(2025, 12, 25, 10, 0, tzinfo=timezone.utc)
    res = pretrade_check({"venue": "TESTEX"}, ts=ts)
    assert res["eligible"] is False and res["reason"] == "holiday"


def test_missing_calendar_ineligible():
    res = pretrade_check(
        {"venue": "NO_SUCH_VENUE"},
        ts=datetime(2025, 12, 15, 12, 0, tzinfo=timezone.utc),
    )
    assert res["eligible"] is False and res["reason"] == "missing_calendar"
