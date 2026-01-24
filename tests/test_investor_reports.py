from datetime import datetime, timedelta, timezone

import pytest

from octa_reports.investor_reports import generate_investor_report


def make_nav_series(start_nav=100.0, days=10, daily_growth=0.01):
    out = []
    now = datetime.now(timezone.utc)
    nav = start_nav
    for i in range(days + 1):
        out.append({"date": (now + timedelta(days=i)).isoformat(), "nav": nav})
        nav = nav * (1.0 + daily_growth)
    return out


def test_report_reconciles_nav_and_fees():
    nav_series = make_nav_series(start_nav=100.0, days=9, daily_growth=0.005)
    fees = [
        {"date": nav_series[2]["date"], "type": "management", "amount": 1.0},
        {"date": nav_series[5]["date"], "type": "performance", "amount": 2.5},
    ]
    rpt = generate_investor_report("inv1", nav_series, fees)

    # nav_end equals last nav provided
    assert rpt.nav_end == pytest.approx(nav_series[-1]["nav"])
    # fees_total equals sum of fees
    assert rpt.fees_total == pytest.approx(3.5)
    # total_return consistent with nav start/end
    expected_total = (nav_series[-1]["nav"] / nav_series[0]["nav"]) - 1.0
    assert rpt.total_return == pytest.approx(expected_total)


def test_risk_metrics_and_drawdown():
    # construct a NAV series with a drawdown
    now = datetime.now(timezone.utc)
    nav_series = [
        {"date": (now + timedelta(days=0)).isoformat(), "nav": 100.0},
        {"date": (now + timedelta(days=1)).isoformat(), "nav": 110.0},
        {"date": (now + timedelta(days=2)).isoformat(), "nav": 90.0},
        {"date": (now + timedelta(days=3)).isoformat(), "nav": 95.0},
    ]
    rpt = generate_investor_report("inv2", nav_series, [])
    # max drawdown should be (110-90)/110 = 20/110 ~= 0.1818
    assert rpt.max_drawdown == pytest.approx((110.0 - 90.0) / 110.0)
    # volatility should be a positive number
    assert rpt.volatility_annual is None or rpt.volatility_annual >= 0.0
