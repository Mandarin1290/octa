from octa_ledger.core import AuditChain
from octa_strategy.capacity import CapacityEngine
from octa_strategy.evidence_pack import EvidencePackBuilder
from octa_strategy.health import HealthScorer
from octa_strategy.models import StrategyMeta
from octa_strategy.registry import StrategyRegistry
from octa_strategy.risk_budget import RiskBudget, RiskBudgetEngine


def test_evidence_pack_reproducible_and_audited():
    ledger = AuditChain()

    def audit_fn(event, payload):
        ledger.append({"event": event, **payload})

    registry = StrategyRegistry(audit_fn=audit_fn)
    s = StrategyMeta(
        strategy_id="SX1",
        owner="ops",
        asset_classes=["equity"],
        risk_budget=1.0,
        holding_period_days=30,
        expected_turnover_per_month=2.0,
        lifecycle_state="LIVE",
    )
    registry.register(s)

    rengine = RiskBudgetEngine(audit_fn=audit_fn)
    budget = RiskBudget(vol_budget=1.0, dd_budget=100.0, exposure_budget=1000.0)
    rengine.register_strategy("SX1", budget)
    rengine.record_usage("SX1", vol=0.2, dd=5.0, exposure=200.0)

    cengine = CapacityEngine(audit_fn=audit_fn)
    cengine.register_strategy("SX1", adv=100000, turnover=1.0, impact=0.1)
    cengine.allocate("SX1", 200.0)

    scorer = HealthScorer()

    builder = EvidencePackBuilder(
        registry=registry,
        ledger=ledger,
        audit_fn=audit_fn,
        risk_engine=rengine,
        capacity_engine=cengine,
        health_scorer=scorer,
    )

    pack1 = builder.generate("SX1")
    pack2 = builder.generate("SX1")

    # reproducible pack id and content (pack_id included in pack)
    assert pack1["pack_id"] == pack2["pack_id"]
    assert pack1["registry_meta"]["strategy_id"] == "SX1"

    # ledger contains evidence_pack.generated for the last pack
    found = [
        b.payload
        for b in ledger._chain
        if isinstance(b.payload, dict)
        and b.payload.get("event") == "evidence_pack.generated"
    ]
    assert any(p.get("pack_id") == pack1["pack_id"] for p in found)
