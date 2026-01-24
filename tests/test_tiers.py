from octa_capital.aum_state import AUMState
from octa_capital.tiers import CapitalTierEngine, Tier
from octa_ledger.core import AuditChain


def test_tier_selection_logic():
    engine = CapitalTierEngine(thresholds={"seed_max": 1_000.0, "growth_max": 10_000.0})
    assert engine.determine_tier(500.0) == Tier.SEED
    assert engine.determine_tier(1_000.0) == Tier.SEED
    assert engine.determine_tier(5_000.0) == Tier.GROWTH
    assert engine.determine_tier(10_000.0) == Tier.GROWTH
    assert engine.determine_tier(10_001.0) == Tier.INSTITUTIONAL


def test_tier_transition_logged():
    ledger = AuditChain()
    aum = AUMState(
        audit_fn=lambda e, p: ledger.append({"event": e, **p}),
        initial_internal=500.0,
        initial_external=0.0,
    )
    engine = CapitalTierEngine(
        thresholds={"seed_max": 1000.0, "growth_max": 10000.0},
        audit_fn=lambda e, p: ledger.append({"event": e, **p}),
    )
    engine.attach(aum)

    # initial snapshot should set SEED
    aum.snapshot(portfolio_value=500.0)
    assert engine.get_current_tier().value == "SEED"

    # inflow pushes to GROWTH
    aum.inflow(2000.0, source="external", reason="capital")
    aum.snapshot(portfolio_value=2500.0)
    assert engine.get_current_tier().value == "GROWTH"

    # large inflow pushes to INSTITUTIONAL
    aum.inflow(10000.0, source="external", reason="capital")
    aum.snapshot(portfolio_value=12500.0)
    assert engine.get_current_tier().value == "INSTITUTIONAL"

    # audit chain contains transition events (filter by event name)
    [
        b.payload.get("event") if isinstance(b.payload, dict) else None
        for b in ledger._chain
    ]
    # We expect at least three transition events stored via engine.audit_fn
    transitions = [
        blk
        for blk in ledger._chain
        if isinstance(blk.payload, dict) and blk.payload.get("new_tier")
    ]
    assert len(transitions) >= 3
