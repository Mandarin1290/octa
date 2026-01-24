from octa_alpha.audit import AuditChain
from octa_alpha.failure_modes import FailureModeRegistry
from octa_alpha.hypotheses import HypothesisRegistry
from octa_alpha.paper_deploy import LifecycleEngine
from octa_reports.alpha_factory import AlphaFactoryDashboard


def test_dashboard_reconciles_with_pipeline():
    hr = HypothesisRegistry()
    h1 = hr.register(
        economic_intuition="m1",
        expected_regime="r1",
        expected_failure_modes="f1",
        risk_assumptions="ra",
        test_spec={"t": 1},
    )
    h2 = hr.register(
        economic_intuition="m2",
        expected_regime="r2",
        expected_failure_modes="f2",
        risk_assumptions="ra",
        test_spec={"t": 2},
    )

    ac = AuditChain()
    # simulate pipeline logs
    ac.append("stage.0", {"hypothesis_id": h1.hypothesis_id})
    ac.append("stage.1", {"hypothesis_id": h1.hypothesis_id})
    ac.append("stage.1", {"hypothesis_id": h2.hypothesis_id})
    ac.append(
        "stage.2.rejected",
        {"hypothesis_id": h2.hypothesis_id, "reason": "underpowered"},
    )

    le = LifecycleEngine()
    le.register("d1", "PAPER", 1000)

    fm = FailureModeRegistry(taxonomy=["mean_reversion"])
    fm.observe(h1.hypothesis_id, ["mean_reversion"])
    fm.observe(h2.hypothesis_id, ["unexpected_mode"])

    dash = AlphaFactoryDashboard(
        hypothesis_registry=hr, audit_chain=ac, lifecycle_engine=le, failure_registry=fm
    )
    summary = dash.summary()

    assert len(summary["active_hypotheses"]) == 2
    counts = summary["pipeline_stage_counts"]
    assert counts.get("stage.1", 0) == 2
    rej = summary["rejection_reasons"]
    assert "underpowered" in rej
    deployments = summary["paper_deployments"]
    assert len(deployments) == 1 and deployments[0]["state"] == "PAPER"
    fs = summary["failure_stats"]
    assert fs["total_events"] == 2
    assert fs["unexpected_total"] == 1
