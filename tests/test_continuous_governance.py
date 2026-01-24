from datetime import datetime, timedelta, timezone

from octa_governance.continuous_review import ContinuousReviewLoop


def test_reviews_triggered_and_audit_logged():
    loop = ContinuousReviewLoop()
    r1 = loop.trigger_daily_risk_review(participants=["ops"], notes="daily check")
    r2 = loop.trigger_weekly_strategy_review(participants=["strat"], notes="weekly")
    r3 = loop.trigger_monthly_committee(participants=["board"], notes="monthly")

    audit = loop.get_audit()
    assert len(audit) >= 3
    cycles = {e["cycle"] for e in audit}
    assert (
        "daily_risk" in cycles
        and "weekly_strategy" in cycles
        and "monthly_committee" in cycles
    )
    for rec in (r1, r2, r3):
        assert rec.evidence_hash and len(rec.evidence_hash) == 64


def test_run_scheduled_executes_due_reviews():
    loop = ContinuousReviewLoop()
    # prime last_run to an old date so scheduled runs will fire
    old = (datetime.now(timezone.utc) - timedelta(days=40)).isoformat()
    loop.last_run["daily_risk"] = old
    loop.last_run["weekly_strategy"] = old
    loop.last_run["monthly_committee"] = old

    out = loop.run_scheduled()
    assert len(out) == 3
    # audit should now contain entries
    assert len(loop.get_audit()) >= 3
