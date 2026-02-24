from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable


class GateDecision(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass(frozen=True)
class GateOutcome:
    decision: GateDecision
    eligible_symbols: list[str] = field(default_factory=list)
    rejected_symbols: list[str] = field(default_factory=list)
    artifacts: Mapping[str, Any] = field(default_factory=dict)
    # I2: is_noop=True marks outcomes from SafeNoopGate; consumers must not treat these
    # as signal-bearing PASSes for promotion purposes.
    is_noop: bool = False


@runtime_checkable
class GateInterface(Protocol):
    name: str
    timeframe: str

    def fit(self, symbols: Sequence[str]) -> None:
        ...

    def evaluate(self, symbols: Sequence[str]) -> GateOutcome:
        ...

    def filter_universe(self, symbols: Sequence[str]) -> Sequence[str]:
        ...

    def emit_artifacts(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        ...


@dataclass(frozen=True)
class GateSpec:
    name: str
    timeframe: str
    gate: GateInterface
