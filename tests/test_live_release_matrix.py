from octa_training.core.live_release_matrix import (
    LiveMode,
    TimeframeOutcome,
    determine_live_release,
)


def _o(tf: str, status: str, expectancy: float | None = 0.01) -> TimeframeOutcome:
    return TimeframeOutcome(
        timeframe=tf,
        status=status,
        sharpe=1.0,
        max_drawdown=0.1,
        expectancy_net=expectancy,
        stability_wf_std=0.2,
        cost_bps=2.0,
        spread_bps=1.0,
    )


def test_case_a_full_mode():
    d = determine_live_release({"1D": _o("1D", "PASS"), "1H": _o("1H", "PASS"), "30m": _o("30m", "PASS")})
    assert d.mode == LiveMode.FULL_MODE
    assert d.position_size_cap == 1.0
    assert d.intraday_allowed is True
    assert "30m" in d.timing_sources
    assert d.micro_layer_weight_1m == 0.0


def test_case_b_reduced_intraday_mode():
    d = determine_live_release({"1D": _o("1D", "PASS"), "1H": _o("1H", "PASS"), "30m": _o("30m", "FAIL")})
    assert d.mode == LiveMode.REDUCED_INTRADAY_MODE
    assert d.position_size_cap == 0.5
    assert d.intraday_allowed is True
    assert d.reentries_allowed is False


def test_case_c_defensive_daily_mode():
    d = determine_live_release({"1D": _o("1D", "PASS"), "1H": _o("1H", "FAIL"), "30m": _o("30m", "PASS")})
    assert d.mode == LiveMode.DEFENSIVE_DAILY_MODE
    assert d.position_size_cap == 0.1
    assert d.intraday_allowed is False
    assert d.min_holding_days == 1


def test_case_c_when_30m_missing_under_strict_cascade():
    d = determine_live_release({"1D": _o("1D", "PASS"), "1H": _o("1H", "FAIL")})
    assert d.mode == LiveMode.DEFENSIVE_DAILY_MODE


def test_case_b_when_30m_missing_under_strict_cascade():
    d = determine_live_release({"1D": _o("1D", "PASS"), "1H": _o("1H", "PASS")})
    assert d.mode == LiveMode.REDUCED_INTRADAY_MODE


def test_case_d_no_trade_mode():
    d = determine_live_release({"1D": _o("1D", "FAIL"), "1H": _o("1H", "PASS"), "30m": _o("30m", "PASS")})
    assert d.mode == LiveMode.NO_TRADE_MODE
    assert d.position_size_cap == 0.0
    assert d.entries_allowed is False


def test_no_trade_if_expectancy_unknown_fail_closed():
    d = determine_live_release({"1D": _o("1D", "PASS", expectancy=None), "1H": _o("1H", "PASS"), "30m": _o("30m", "PASS")})
    assert d.mode == LiveMode.NO_TRADE_MODE


def test_no_trade_if_1h_expectancy_not_positive_in_intraday_modes():
    d = determine_live_release({"1D": _o("1D", "PASS", expectancy=0.01), "1H": _o("1H", "PASS", expectancy=0.0), "30m": _o("30m", "PASS")})
    assert d.mode == LiveMode.NO_TRADE_MODE
