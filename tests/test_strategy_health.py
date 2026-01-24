from octa_ledger.core import AuditChain
from octa_reports.strategy_health import StrategyHealthDashboard
from octa_strategy.alpha_decay import AlphaDecayDetector
from octa_strategy.health import HealthScorer
from octa_strategy.models import StrategyMeta
from octa_strategy.regime_fit import RegimeFitEngine
from octa_strategy.registry import StrategyRegistry
from octa_strategy.stability import PerformanceStabilityAnalyzer


def test_dashboard_reconciles_with_metrics():
    ledger = AuditChain()

    def audit_fn(event, payload):
        ledger.append({"event": event, **payload})

    registry = StrategyRegistry(audit_fn=audit_fn)
    # create two strategies with created_at in the past for deterministic tiers
    from datetime import datetime, timedelta

    now = datetime.utcnow()
    s1 = StrategyMeta(
        strategy_id="A1",
        owner="ops",
        asset_classes=["equity"],
        risk_budget=1.0,
        holding_period_days=30,
        expected_turnover_per_month=2.0,
        lifecycle_state="LIVE",
        created_at=(now - timedelta(days=30)).isoformat() + "Z",
    )
    s2 = StrategyMeta(
        strategy_id="A2",
        owner="ops",
        asset_classes=["equity"],
        risk_budget=1.0,
        holding_period_days=30,
        expected_turnover_per_month=2.0,
        lifecycle_state="LIVE",
        created_at=(now - timedelta(days=400)).isoformat() + "Z",
    )
    registry.register(s1)
    registry.register(s2)

    # detectors/engines
    health = HealthScorer()
    alpha = AlphaDecayDetector(baseline_window=50, recent_window=10)
    regime = RegimeFitEngine()
    stability = PerformanceStabilityAnalyzer(baseline_window=50, recent_window=10)

    # returns: A1 stable, A2 deteriorating
    import random

    rnd = random.Random(1)
    returns_a1 = [rnd.gauss(0.001, 0.0005) for _ in range(200)]
    returns_a2 = [rnd.gauss(0.001, 0.0005) for _ in range(150)] + [
        rnd.gauss(-0.002, 0.001) for _ in range(50)
    ]

    dashboard = StrategyHealthDashboard(
        registry=registry,
        ledger=ledger,
        health_scorer=health,
        alpha_detector=alpha,
        regime_engine=regime,
        stability_analyzer=stability,
    )
    report = dashboard.build(
        returns_by_strategy={"A1": returns_a1, "A2": returns_a2},
        market_indicator=[0.1] * 100 + [1.0] * 50,
    )

    # reconcile: both strategies present
    ids = [s["strategy_id"] for s in report["strategies"]]
    assert set(ids) == {"A1", "A2"}

    # age tiers deterministic: A1 young, A2 old
    by_id = {s["strategy_id"]: s for s in report["strategies"]}
    assert by_id["A1"]["age_tier"] == "YOUNG"
    assert by_id["A2"]["age_tier"] == "OLD"

    # health scores present and reproducible numeric
    assert isinstance(by_id["A1"]["health_score"], float)
    assert isinstance(by_id["A2"]["health_score"], float)
