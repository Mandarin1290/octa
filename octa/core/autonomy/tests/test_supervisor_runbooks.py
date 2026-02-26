from __future__ import annotations


from octa.core.autonomy.health import HealthLevel, HealthReport, SubsystemHealth
from octa.core.autonomy.runbooks import RunbookConfig, RunbookActionType, select_runbook


def test_audit_failure_runbook() -> None:
    report = HealthReport(
        overall=HealthLevel.CRITICAL,
        subsystems={
            "audit": SubsystemHealth("audit", HealthLevel.CRITICAL, "AUDIT_WRITE_FAIL", {}, None)
        },
        recommended_mode="SAFE",
        reasons=["AUDIT_WRITE_FAIL"],
    )
    plan = select_runbook(report, "AAA", set(), RunbookConfig())
    assert plan is not None
    assert any(action.type == RunbookActionType.HALT for action in plan.actions)


def test_data_staleness_runbook_quarantine() -> None:
    report = HealthReport(
        overall=HealthLevel.DEGRADED,
        subsystems={
            "data": SubsystemHealth("data", HealthLevel.DEGRADED, "TOO_FEW_BARS", {"bars": 1}, None)
        },
        recommended_mode="DEGRADED",
        reasons=["TOO_FEW_BARS"],
    )
    plan = select_runbook(report, "AAA", set(), RunbookConfig())
    assert plan is not None
    assert any(action.type == RunbookActionType.QUARANTINE_SYMBOL for action in plan.actions)
