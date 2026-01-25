from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class RobustnessResult:
    score: float


def robustness_score(metrics: Mapping[str, float]) -> RobustnessResult:
    stability = float(metrics.get("stability", 0.5))
    return RobustnessResult(score=max(0.0, min(1.0, stability)))
