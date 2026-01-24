from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List


@dataclass
class StrategySpec:
    id: str
    universe: List[str]
    frequency: str
    risk_budget: float


@dataclass
class StrategyInput:
    timestamp: str
    features: Dict[str, float]
    prices: Dict[str, List[float]]
    regime: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class StrategyOutput:
    exposures: Dict[str, float]
    confidence: float
    rationale: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
