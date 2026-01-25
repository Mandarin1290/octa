from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class CapacityResult:
    pass_flag: bool
    max_participation: float


def capacity_check(metrics: Mapping[str, float], max_participation: float = 0.1) -> CapacityResult:
    turnover = float(metrics.get("turnover", 0.0))
    return CapacityResult(pass_flag=turnover <= max_participation, max_participation=max_participation)
