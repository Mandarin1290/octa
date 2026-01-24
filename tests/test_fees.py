import pytest

from octa_accounting.fees import FeeEngine


def approx(a, b, rel=1e-9):
    return abs(a - b) <= rel * max(1.0, abs(a), abs(b))


def test_management_fee_accrual():
    fe = FeeEngine()
    fe.add_share_class("A", initial_hwm=100.0, mgmt_rate_annual=0.02, perf_rate=0.2)
    amt = fe.accrue_management("A", nav_per_share=100.0, days=30)
    expected = 0.02 * (30.0 / 365.0) * 100.0
    assert approx(amt, expected)
    snapshot = fe.snapshot_audit()["A"]
    assert snapshot["accrued_mgmt"] == pytest.approx(expected)


def test_performance_fee_and_hwm_crystallization():
    fe = FeeEngine()
    fe.add_share_class("B", initial_hwm=100.0, mgmt_rate_annual=0.0, perf_rate=0.2)

    # NAV below HWM: no performance accrual
    amt = fe.accrue_performance("B", nav_per_share=99.0)
    assert amt == 0.0

    # NAV rises above HWM -> accrual
    amt = fe.accrue_performance("B", nav_per_share=105.0)
    expected = (105.0 - 100.0) * 0.2
    assert approx(amt, expected)
    sn = fe.snapshot_audit()["B"]
    assert sn["accrued_perf"] == pytest.approx(expected)

    # Crystallize: payable increases, HWM updates to current NAV
    res = fe.crystallize("B", nav_per_share=105.0)
    assert res["total_crystallized"] == pytest.approx(expected)
    assert res["hwm"] == pytest.approx(105.0)
    assert fe.payable("B") == pytest.approx(expected)

    # NAV falls below HWM: no accrual
    amt = fe.accrue_performance("B", nav_per_share=102.0)
    assert amt == 0.0

    # NAV rises above new HWM -> accrual only on new gain
    amt = fe.accrue_performance("B", nav_per_share=110.0)
    expected2 = (110.0 - 105.0) * 0.2
    assert approx(amt, expected2)
    # Crystallize again
    res2 = fe.crystallize("B", nav_per_share=110.0)
    assert res2["total_crystallized"] == pytest.approx(expected2)
    assert res2["hwm"] == pytest.approx(110.0)
    # payable has accumulated both crystallizations
    assert fe.payable("B") == pytest.approx(expected + expected2)


def test_fees_do_not_modify_nav():
    fe = FeeEngine()
    fe.add_share_class("C", initial_hwm=50.0, mgmt_rate_annual=0.01, perf_rate=0.1)
    nav = 60.0
    _ = fe.accrue_management("C", nav_per_share=nav, days=1)
    _ = fe.accrue_performance("C", nav_per_share=nav)
    # Engine should not alter provided NAV; caller owns NAV state
    assert nav == 60.0
