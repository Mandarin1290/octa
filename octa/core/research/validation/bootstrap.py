from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class BootstrapResult:
    sharpe_ci_low: float
    sharpe_ci_high: float


def bootstrap_metrics(metrics: Mapping[str, float]) -> BootstrapResult:
    sharpe = float(metrics.get("sharpe", 0.5))
    return BootstrapResult(sharpe_ci_low=sharpe - 0.2, sharpe_ci_high=sharpe + 0.2)
