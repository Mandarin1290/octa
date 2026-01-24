from decimal import Decimal

from octa_alpha.alpha_portfolio import AlphaCandidate, optimize_weights
from octa_alpha.governance import Governance
from octa_reports.strategy_factory_master import StrategyFactoryMaster


def test_dashboard_reconciles_with_subsystems():
    # setup candidates
    a = AlphaCandidate(
        alpha_id="A",
        base_utility=Decimal("1.0"),
        volatility=Decimal("0.05"),
        exposure=[Decimal("1"), Decimal("0")],
    )
    b = AlphaCandidate(
        alpha_id="B",
        base_utility=Decimal("0.8"),
        volatility=Decimal("0.04"),
        exposure=[Decimal("0"), Decimal("1")],
    )
    candidates = [a, b]

    gov = Governance()
    gov.submit_for_approval("A")
    gov.approve("A")
    gov.submit_for_approval("B")
    gov.veto("B", vetoer="board", reason="risk")

    master = StrategyFactoryMaster(governance=gov)

    signals = {"A": 0.5, "B": 0.2}
    base_conf = {"A": 1.0, "B": 1.0}
    regime = "neutral"
    regime_compat = {"A": {"neutral": 1.0}, "B": {"neutral": 1.0}}
    regime_unc = {"A": 0.0, "B": 0.0}

    dash = master.build_dashboard(
        candidates, signals, base_conf, regime, regime_compat, regime_unc
    )

    # inventory count
    assert len(dash["alpha_inventory"]) == 2

    # crowding matches direct call
    from octa_alpha.crowding import CrowdingProfile, crowding_index

    profiles = [
        CrowdingProfile(alpha_id=c.alpha_id, exposure=c.exposure) for c in candidates
    ]
    crowd = crowding_index(profiles)
    assert dash["crowding"] == crowd

    # allocation map equals optimize_weights result
    weights = optimize_weights(candidates)
    assert dash["allocation_map"] == weights

    # governance interventions reflect veto
    gov_info = dash["governance"]
    assert any(e["action"] == "veto" for e in gov_info["audit_log"])
    assert "B" in gov_info["vetoed"]
