from datetime import datetime, timedelta

from octa_strategy.aging import AgingConfig, AgingEngine
from octa_strategy.lifecycle import StrategyLifecycle, TransitionRecord
from octa_strategy.risk_budget import RiskBudget, RiskBudgetEngine


def make_live_lifecycle(days_live: int) -> StrategyLifecycle:
    lc = StrategyLifecycle(strategy_id="S1")
    # set initial LIVE state record manually in history with timestamp days_live in past
    past = datetime.utcnow() - timedelta(days=days_live)
    rec = TransitionRecord(
        from_state="SHADOW", to_state="LIVE", timestamp=past, doc="go live"
    )
    lc.history.append(rec)
    lc.current_state = "LIVE"
    return lc


def test_tier_changes():
    eng = AgingEngine()
    lc_young = make_live_lifecycle(30)
    lc_mature = make_live_lifecycle(200)
    lc_old = make_live_lifecycle(800)

    assert eng.tier_for(lc_young) == "YOUNG"
    assert eng.tier_for(lc_mature) == "MATURE"
    assert eng.tier_for(lc_old) == "OLD"


def test_thresholds_tighten_correctly():
    cfg = AgingConfig(
        young_days=90,
        mature_days=365,
        young_multiplier=1.0,
        mature_multiplier=0.9,
        old_multiplier=0.8,
    )
    eng = AgingEngine(config=cfg)
    budget = RiskBudget(vol_budget=1.0, dd_budget=100.0, exposure_budget=1000.0)

    young_thresh = eng.adjust_thresholds(budget, "YOUNG")
    mature_thresh = eng.adjust_thresholds(budget, "MATURE")
    old_thresh = eng.adjust_thresholds(budget, "OLD")

    # multipliers applied: mature < young, old < mature
    assert mature_thresh[0] < young_thresh[0]
    assert old_thresh[0] < mature_thresh[0]

    # integrate with RiskBudgetEngine: register and check
    rengine = RiskBudgetEngine(audit_fn=lambda e, p: None)
    rengine.register_strategy("S1", budget)
    # record usage that would trigger different behaviour
    rengine.record_usage("S1", vol=0.95, dd=10.0, exposure=100.0)

    lc = make_live_lifecycle(400)  # mature->old boundary
    res = eng.check_and_escalate("S1", lc, rengine)
    assert "tier" in res
    assert res["tier"] in ("MATURE", "OLD")
