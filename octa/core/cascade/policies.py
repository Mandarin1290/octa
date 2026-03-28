from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from .contracts import GateInterface

DEFAULT_TIMEFRAMES: tuple[str, ...] = ("1D", "1H", "30M", "5M", "1M")
EXTENDED_TIMEFRAMES_WITH_4H: tuple[str, ...] = ("1D", "4H", "1H", "30M", "5M", "1M")


@dataclass(frozen=True)
class CascadePolicy:
    timeframes: Sequence[str] = field(default_factory=lambda: DEFAULT_TIMEFRAMES)
    strict: bool = True

    def order_gates(self, gates: Sequence[GateInterface]) -> list[GateInterface]:
        gates_by_timeframe = {gate.timeframe: gate for gate in gates}
        ordered: list[GateInterface] = []
        for timeframe in self.timeframes:
            if timeframe in gates_by_timeframe:
                ordered.append(gates_by_timeframe[timeframe])
        return ordered
