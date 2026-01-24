from octa_core.capacity import CapacityEngine, IneligibleAsset, LiquidityProfile
from octa_vertex.slicing import vwap_slices


def test_illiquid_asset_smaller_cap():
    prof_liquid = LiquidityProfile(symbol="LIQ", adv=1_000_000, price=10.0)
    prof_illiquid = LiquidityProfile(symbol="ILL", adv=10_000, price=10.0)
    eng = CapacityEngine(max_pct_adv_per_day=0.1, max_impact_bps=50.0)

    cap_liq = eng.compute_max_notional(prof_liquid)
    cap_ill = eng.compute_max_notional(prof_illiquid)
    assert cap_ill < cap_liq


def test_missing_adv_ineligible():
    prof = LiquidityProfile(symbol="X", adv=None, price=5.0)
    eng = CapacityEngine()
    try:
        eng.compute_max_notional(prof)
        raise AssertionError("Expected IneligibleAsset")
    except IneligibleAsset:
        pass


def test_stress_multiplier_reduces_allowed_size():
    prof = LiquidityProfile(symbol="S", adv=100_000, price=20.0)
    eng = CapacityEngine(
        max_pct_adv_per_day=0.1, max_impact_bps=100.0, stress_multiplier=4.0
    )
    normal = eng.compute_max_notional(prof, stress=False)
    stressed = eng.compute_max_notional(prof, stress=True)
    assert stressed * 4 == normal


def test_vwap_slicing_respects_slice_limit_and_tick(tmp_path):
    prof = LiquidityProfile(
        symbol="T", adv=100_000, price=50.0, tick_size=0.01, contract_multiplier=1.0
    )
    eng = CapacityEngine(max_participation_rate=0.05)
    total_notional = 1_000_000  # want to trade 1m
    slices = vwap_slices(prof, total_notional, eng, execution_window_hours=6.5)
    # each slice should be <= slice limit
    slice_limit = eng.compute_slice_limits(prof, execution_window_hours=6.5)
    assert all(s <= slice_limit + 1e-6 for s in slices)
    # tick alignment: each slice should be multiple of tick*price
    tick_notional = prof.tick_size * prof.price * prof.contract_multiplier
    for s in slices:
        assert abs((s / tick_notional) - round(s / tick_notional)) < 1e-6
