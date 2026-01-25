from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class StressResult:
    pass_flag: bool
    worst_drawdown: float


def run_stress(metrics: Mapping[str, float]) -> StressResult:
    base_dd = abs(float(metrics.get("max_drawdown", 0.0)))
    stressed_dd = min(1.0, base_dd * 1.5)
    return StressResult(pass_flag=stressed_dd <= 0.2, worst_drawdown=-stressed_dd)
