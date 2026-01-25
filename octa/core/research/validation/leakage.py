from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class LeakageReport:
    ok: bool
    reason: str


def check_leakage(metrics: Mapping[str, float]) -> LeakageReport:
    leak_score = float(metrics.get("leak_score", 0.0))
    if leak_score > 0.5:
        return LeakageReport(ok=False, reason="leakage_detected")
    return LeakageReport(ok=True, reason="ok")
