from octa_core.capacity import CapacityEngine, LiquidityProfile
from octa_ledger.store import LedgerStore
from octa_sentinel.liquidity_stress import LiquidityStressTester, StressScenario


def test_stress_outputs_stable_and_reproducible(tmp_path):
    lp = tmp_path / "ledger"
    ls = LedgerStore(str(lp))
    eng = CapacityEngine()
    tester = LiquidityStressTester(eng)

    profiles = [
        LiquidityProfile(
            symbol="A",
            adv=100_000,
            price=10.0,
            spread_bps=1.0,
            vol=0.02,
            tick_size=0.01,
        ),
        LiquidityProfile(symbol="B", adv=50_000, price=20.0, spread_bps=2.0, vol=0.03),
    ]
    scenario = StressScenario(
        name="adv50_spread2_vol2_gap5",
        version="v1",
        adv_shock_factor=0.5,
        spread_mult=2.0,
        vol_mult=2.0,
        gap_pct=0.05,
    )
    r1 = tester.run_scenario(
        ls,
        profiles,
        scenario,
        portfolio_notional=1_000_000,
        time_to_liquidate_threshold_days=10.0,
    )
    r2 = tester.run_scenario(
        ls,
        profiles,
        scenario,
        portfolio_notional=1_000_000,
        time_to_liquidate_threshold_days=10.0,
    )
    assert r1 == r2


def test_sentinel_action_triggers_on_breach(tmp_path):
    lp = tmp_path / "ledger"
    ls = LedgerStore(str(lp))
    eng = CapacityEngine()
    LiquidityStressTester(eng)

    # make engine permissive so TTL can exceed threshold (test breach path)
    eng_breach = CapacityEngine(
        max_pct_adv_per_day=2.0,
        max_participation_rate=0.5,
        max_impact_bps=10000.0,
        stress_multiplier=1.0,
    )
    tester_breach = LiquidityStressTester(eng_breach)
    profiles = [
        LiquidityProfile(symbol="C", adv=10, price=100.0, spread_bps=5.0, vol=0.05)
    ]
    scenario = StressScenario(
        name="adv10_gap10",
        version="v1",
        adv_shock_factor=0.5,
        spread_mult=1.0,
        vol_mult=1.0,
        gap_pct=0.1,
    )
    tester_breach.run_scenario(
        ls,
        profiles,
        scenario,
        portfolio_notional=1_000_000,
        time_to_liquidate_threshold_days=1.0,
    )
    # check that a gate_event was appended due to TTL breach
    gates = ls.by_action("gate_event")
    assert gates
