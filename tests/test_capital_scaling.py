from octa_capital.aum_state import AUMState
from octa_capital.hard_close import HardCloseEngine
from octa_capital.liquidity_buckets import LiquidityBuckets
from octa_capital.scaling_impact import ScalingImpactAnalyzer
from octa_capital.soft_close import SoftCloseEngine
from octa_capital.tiers import CapitalTierEngine
from octa_core.aum_allocator import (
    AUMAwareAllocator,
    StrategyCapacitySpec,
    inverse_scale_factory,
)
from octa_reports.capital_scaling import CapitalScalingDashboard


def test_dashboard_reconciles_with_aum_state():
    def ledger_fn(e, p):
        return None

    aum = AUMState(
        audit_fn=ledger_fn, initial_internal=1_000_000.0, initial_external=0.0
    )
    tier = CapitalTierEngine(
        thresholds={"seed_max": 500_000.0, "growth_max": 5_000_000.0},
        audit_fn=ledger_fn,
    )
    tier.attach(aum)
    # emit initial snapshot so tier engine records current tier
    aum.snapshot(portfolio_value=aum.get_current_total())
    soft = SoftCloseEngine(thresholds={"capacity_utilization": 0.9}, audit_fn=ledger_fn)
    soft.attach(aum)
    hard = HardCloseEngine(
        absolute_cap=10_000_000.0, required_approvals=1, audit_fn=ledger_fn
    )
    hard.attach(aum)
    liquidity = LiquidityBuckets()
    scaling = ScalingImpactAnalyzer(beta=0.5, audit_fn=ledger_fn)

    # simple allocator specs for capacity computation
    specs = {
        "S1": StrategyCapacitySpec(
            base_fraction_of_aum=0.02,
            scale_fn=inverse_scale_factory(reference_aum=1_000_000.0),
        ),
        "S2": StrategyCapacitySpec(
            base_fraction_of_aum=0.01,
            scale_fn=inverse_scale_factory(reference_aum=1_000_000.0),
        ),
    }
    allocator = AUMAwareAllocator(capacity_specs=specs, deploy_fraction=0.8)

    # positions with liquidity and historical returns
    positions = {
        "S1": {"weight": 0.6, "liquidity_days": 1.0, "historical_return": 100.0},
        "S2": {"weight": 0.4, "liquidity_days": 10.0, "historical_return": 50.0},
    }

    dashboard = CapitalScalingDashboard(
        aum_state=aum,
        tier_engine=tier,
        soft_close=soft,
        hard_close=hard,
        liquidity=liquidity,
        scaling=scaling,
        allocator=allocator,
    )
    report = dashboard.build(
        positions=positions,
        expected_returns={"S1": 1.0, "S2": 0.5},
        base_aum=1_000_000.0,
        hurdle_rate=0.00001,
    )

    assert report["current_aum"] == aum.get_current_total()
    assert report["capital_tier"] == tier.get_current_tier().value
    assert isinstance(report["liquidity_buckets"], dict)
    assert report["worst_bucket"] is not None
    # scaling_break_even_aum may be None or number; headroom reconciles when present
    if report["scaling_break_even_aum"] is not None:
        assert (
            abs(
                report["scaling_headroom"]
                - (report["scaling_break_even_aum"] - report["current_aum"])
            )
            < 1e-6
        )
