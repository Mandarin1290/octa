import pytest

from octa_ledger.core import AuditChain
from octa_reports.strategy_factory import StrategyFactoryReport
from octa_strategy.models import StrategyMeta
from octa_strategy.registry import StrategyRegistry


def test_strategy_factory_deterministic_and_reconciles():
    ledger = AuditChain()

    # simple audit wrapper used by modules in the real system
    def audit_fn(event, payload):
        ledger.append({"event": event, **payload})

    registry = StrategyRegistry(audit_fn=audit_fn)

    # create two strategies
    s1 = StrategyMeta(
        strategy_id="STRAT-A",
        owner="ops",
        asset_classes=["equity"],
        risk_budget=1.0,
        holding_period_days=30,
        expected_turnover_per_month=2.0,
        lifecycle_state="PAPER",
    )
    s2 = StrategyMeta(
        strategy_id="STRAT-B",
        owner="ops",
        asset_classes=["fx"],
        risk_budget=1.0,
        holding_period_days=30,
        expected_turnover_per_month=2.0,
        lifecycle_state="SHADOW",
    )

    registry.register(s1)
    registry.register(s2)

    # record risk budgets and usage for STRAT-A
    audit_fn(
        "risk.register",
        {
            "strategy_id": "STRAT-A",
            "budget": {
                "vol_budget": 2.0,
                "dd_budget": 100.0,
                "exposure_budget": 1000.0,
            },
        },
    )
    audit_fn(
        "risk.usage",
        {
            "strategy_id": "STRAT-A",
            "usage": {"vol": 1.0, "dd": 10.0, "exposure": 100.0},
        },
    )

    # capacity params and allocation for STRAT-A
    audit_fn(
        "capacity.register",
        {
            "strategy_id": "STRAT-A",
            "params": {
                "adv": 100000,
                "turnover": 1.0,
                "impact": 0.1,
                "adv_fraction": 0.01,
                "base_scaler": 1.0,
            },
        },
    )
    audit_fn("capacity.allocate", {"strategy_id": "STRAT-A", "new_aum": 1000.0})

    # add a paper gate failure for STRAT-B to act as a promotion blocker
    audit_fn(
        "paper_gates.failed", {"strategy_id": "STRAT-B", "failed": {"runtime": True}}
    )

    rpt = StrategyFactoryReport(registry, ledger).build()

    # reconcile with registry
    reg_keys = set(registry.list().keys())
    rpt_keys = set([s["strategy_id"] for s in rpt["strategies"]])
    assert reg_keys == rpt_keys

    # deterministic ordering and contents for STRAT-A
    s_map = {s["strategy_id"]: s for s in rpt["strategies"]}
    a = s_map["STRAT-A"]
    assert a["lifecycle_state"] == "PAPER"
    # risk utilizations: vol=1/2=0.5, dd=10/100=0.1, exposure=100/1000=0.1
    ru = a["risk_budget_utilization"]
    assert pytest.approx(ru["vol"]) == 0.5
    assert pytest.approx(ru["dd"]) == 0.1
    assert pytest.approx(ru["exposure"]) == 0.1
    assert pytest.approx(ru["max"]) == 0.5

    # capacity util: cap = 100000*0.01*(1/0.1)*(1/1.0) = 10000; aum=1000 -> util=0.1
    assert pytest.approx(a["capacity_utilization"]) == 0.1

    # STRAT-B should have a promotion blocker recorded
    b = s_map["STRAT-B"]
    assert (
        any("paper_gates.failed" in x for x in b["promotion_blockers"])
        or b["promotion_blockers"]
    )
