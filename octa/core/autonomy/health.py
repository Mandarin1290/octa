from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from octa.core.data.providers.ohlcv import OHLCVProvider, Timeframe


class HealthLevel(str, Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class SubsystemHealth:
    name: str
    level: HealthLevel
    detail: str
    metrics: dict[str, float]
    last_ok_ts: float | None


@dataclass(frozen=True)
class HealthReport:
    overall: HealthLevel
    subsystems: dict[str, SubsystemHealth]
    recommended_mode: str
    reasons: list[str]


def check_provider(
    provider: OHLCVProvider, symbol: str, timeframe: Timeframe, min_bars: int = 10
) -> SubsystemHealth:
    bars = provider.get_ohlcv(symbol, timeframe)
    if not bars:
        return SubsystemHealth(
            name="data",
            level=HealthLevel.CRITICAL,
            detail="NO_DATA",
            metrics={"bars": 0},
            last_ok_ts=None,
        )
    if len(bars) < min_bars:
        return SubsystemHealth(
            name="data",
            level=HealthLevel.DEGRADED,
            detail="TOO_FEW_BARS",
            metrics={"bars": float(len(bars))},
            last_ok_ts=None,
        )
    last_ts = bars[-1].ts.timestamp()
    return SubsystemHealth(
        name="data",
        level=HealthLevel.OK,
        detail="OK",
        metrics={"bars": float(len(bars))},
        last_ok_ts=last_ts,
    )


def check_audit_writable(path: Path) -> SubsystemHealth:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write("")
    except Exception:
        return SubsystemHealth(
            name="audit",
            level=HealthLevel.CRITICAL,
            detail="AUDIT_WRITE_FAIL",
            metrics={},
            last_ok_ts=None,
        )
    return SubsystemHealth(
        name="audit",
        level=HealthLevel.OK,
        detail="OK",
        metrics={},
        last_ok_ts=datetime.utcnow().timestamp(),
    )


def check_recent_errors(error_counters: Mapping[str, int], threshold: int = 1) -> SubsystemHealth:
    total = sum(error_counters.values()) if error_counters else 0
    if total >= threshold:
        return SubsystemHealth(
            name="cascade",
            level=HealthLevel.DEGRADED,
            detail="ERRORS_DETECTED",
            metrics={"errors": float(total)},
            last_ok_ts=None,
        )
    return SubsystemHealth(
        name="cascade",
        level=HealthLevel.OK,
        detail="OK",
        metrics={"errors": float(total)},
        last_ok_ts=datetime.utcnow().timestamp(),
    )


def summarize_health(subsystems: Sequence[SubsystemHealth]) -> HealthReport:
    subs = {health.name: health for health in subsystems}
    levels = [health.level for health in subsystems]
    reasons = [health.detail for health in subsystems if health.level != HealthLevel.OK]
    if HealthLevel.CRITICAL in levels:
        overall = HealthLevel.CRITICAL
        mode = "SAFE"
    elif HealthLevel.DEGRADED in levels:
        overall = HealthLevel.DEGRADED
        mode = "DEGRADED"
    else:
        overall = HealthLevel.OK
        mode = "NORMAL"
    return HealthReport(
        overall=overall,
        subsystems=subs,
        recommended_mode=mode,
        reasons=reasons,
    )
