from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class StressScenario:
    scenario_id: str
    name: str
    description: str
    shocks: Dict[str, float]


def default_scenarios() -> List[StressScenario]:
    """Conservative macro/curve shocks (configurable by callers).

    Values are dimensionless or in bps depending on key.
    """

    return [
        StressScenario(
            scenario_id="rates_up_100bp",
            name="Rates +100bp",
            description="Parallel rate up-shock to discount curve.",
            shocks={"rates_parallel_bp": 100.0},
        ),
        StressScenario(
            scenario_id="rates_down_100bp",
            name="Rates -100bp",
            description="Parallel rate down-shock to discount curve.",
            shocks={"rates_parallel_bp": -100.0},
        ),
        StressScenario(
            scenario_id="equity_gap_down_5pct",
            name="Equity gap -5%",
            description="Instantaneous equity shock.",
            shocks={"equity_spot_pct": -0.05},
        ),
    ]


__all__ = ["StressScenario", "default_scenarios"]
