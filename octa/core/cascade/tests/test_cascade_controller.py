from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from octa.core.cascade.contracts import GateDecision, GateOutcome
from octa.core.cascade.controller import CascadeController
from octa.core.cascade.policies import CascadePolicy


@dataclass
class DummyGate:
    name: str
    timeframe: str
    decision: GateDecision
    eligible: list[str]
    calls: list[str] = field(default_factory=list)

    def fit(self, symbols: Sequence[str]) -> None:
        self.calls.append(f"fit:{self.name}:{len(symbols)}")

    def evaluate(self, symbols: Sequence[str]) -> GateOutcome:
        self.calls.append(f"evaluate:{self.name}:{len(symbols)}")
        return GateOutcome(
            decision=self.decision,
            eligible_symbols=list(self.eligible),
        )

    def filter_universe(self, symbols: Sequence[str]) -> Sequence[str]:
        self.calls.append(f"filter:{self.name}:{len(symbols)}")
        return list(symbols)

    def emit_artifacts(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        self.calls.append(f"emit:{self.name}:{len(symbols)}")
        return {"gate": self.name}


def test_cascade_flow_order_and_routing() -> None:
    gate_1d = DummyGate("gate_1d", "1D", GateDecision.PASS, ["A", "B"])
    gate_30m = DummyGate("gate_30m", "30M", GateDecision.FAIL, ["A"])
    gate_1h = DummyGate("gate_1h", "1H", GateDecision.PASS, ["A"])

    controller = CascadeController(
        gates=[gate_1d, gate_30m, gate_1h],
        policy=CascadePolicy(timeframes=("1D", "30M", "1H")),
    )

    context = controller.run(["A", "B", "C"])

    assert gate_1d.calls
    assert gate_30m.calls
    assert not gate_1h.calls

    assert context.decisions["gate_1d"]["A"] == GateDecision.PASS
    assert context.decisions["gate_30m"]["A"] == GateDecision.FAIL
    assert set(context.rejected["gate_30m"]) == {"A", "B"}
    assert "cascade_complete:empty_universe" in context.trace
