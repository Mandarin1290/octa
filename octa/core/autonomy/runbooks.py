from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from .health import HealthReport, HealthLevel


class RunbookActionType(str, Enum):
    RETRY = "RETRY"
    BACKOFF = "BACKOFF"
    RESET_PROVIDER = "RESET_PROVIDER"
    QUARANTINE_SYMBOL = "QUARANTINE_SYMBOL"
    REDUCE_UNIVERSE = "REDUCE_UNIVERSE"
    DISABLE_OMS = "DISABLE_OMS"
    SAFE_MODE = "SAFE_MODE"
    HALT = "HALT"


@dataclass(frozen=True)
class RunbookAction:
    type: RunbookActionType
    detail: str
    params: dict[str, Any]


@dataclass(frozen=True)
class RunbookPlan:
    actions: list[RunbookAction]
    next_mode: str
    halt: bool


@dataclass(frozen=True)
class RunbookConfig:
    backoff_schedule_s: tuple[int, int, int, int] = (1, 2, 5, 10)
    max_quarantined_symbols: int = 5
    oms_failure_threshold: int = 1


def data_staleness_runbook(
    symbol: str,
    quarantined: set[str],
    config: RunbookConfig,
    *,
    allow_provider_reset: bool = False,
) -> RunbookPlan:
    actions = [
        RunbookAction(RunbookActionType.BACKOFF, "BACKOFF", {"seconds": config.backoff_schedule_s[0]}),
        RunbookAction(RunbookActionType.QUARANTINE_SYMBOL, "QUARANTINE", {"symbol": symbol}),
    ]
    if allow_provider_reset:
        actions.insert(0, RunbookAction(RunbookActionType.RESET_PROVIDER, "RESET_PROVIDER", {}))
    next_mode = "DEGRADED"
    if len(quarantined) + 1 >= config.max_quarantined_symbols:
        actions.append(RunbookAction(RunbookActionType.SAFE_MODE, "SAFE_MODE", {}))
        next_mode = "SAFE"
    return RunbookPlan(actions=actions, next_mode=next_mode, halt=False)


def audit_failure_runbook() -> RunbookPlan:
    return RunbookPlan(
        actions=[RunbookAction(RunbookActionType.HALT, "AUDIT_FAIL", {})],
        next_mode="SAFE",
        halt=True,
    )


def oms_failure_runbook() -> RunbookPlan:
    return RunbookPlan(
        actions=[
            RunbookAction(
                RunbookActionType.DISABLE_OMS,
                "DISABLE_OMS",
                {"execution_mode": "DECISIONS_ONLY"},
            )
        ],
        next_mode="SAFE",
        halt=False,
    )


def exception_storm_runbook(config: RunbookConfig) -> RunbookPlan:
    return RunbookPlan(
        actions=[
            RunbookAction(RunbookActionType.SAFE_MODE, "SAFE_MODE", {}),
            RunbookAction(RunbookActionType.BACKOFF, "BACKOFF", {"seconds": config.backoff_schedule_s[0]}),
        ],
        next_mode="SAFE",
        halt=False,
    )


def select_runbook(
    report: HealthReport,
    symbol: str,
    quarantined: set[str],
    config: RunbookConfig,
    *,
    allow_provider_reset: bool = False,
) -> RunbookPlan | None:
    audit = report.subsystems.get("audit")
    data = report.subsystems.get("data")

    if audit and audit.level == HealthLevel.CRITICAL:
        return audit_failure_runbook()
    if data and data.level in {HealthLevel.CRITICAL, HealthLevel.DEGRADED}:
        return data_staleness_runbook(
            symbol, quarantined, config, allow_provider_reset=allow_provider_reset
        )
    if report.overall == HealthLevel.CRITICAL:
        return exception_storm_runbook(config)
    return None
