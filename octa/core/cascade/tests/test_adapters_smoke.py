from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from octa.core.cascade.adapters import (
    GlobalRegimeGateAdapter,
    SafeNoopGate,
)
from octa.core.cascade.contracts import GateDecision, GateInterface


@dataclass
class DummyDelegate:
    name: str = "dummy"
    timeframe: str = "1D"
    calls: list[str] = field(default_factory=list)

    def fit(self, symbols: Sequence[str]) -> None:
        self.calls.append("fit")

    def evaluate(self, symbols: Sequence[str]) -> GateDecision:
        self.calls.append("evaluate")
        return GateDecision.PASS

    def filter_universe(self, symbols: Sequence[str]) -> Sequence[str]:
        self.calls.append("filter")
        return list(symbols)

    def emit_artifacts(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        self.calls.append("emit")
        return {"ok": True}


def test_adapter_satisfies_gate_interface() -> None:
    adapter = GlobalRegimeGateAdapter(delegate=DummyDelegate())
    assert isinstance(adapter, GateInterface)

    outcome = adapter.evaluate(["A", "B"])
    assert outcome.decision == GateDecision.PASS
    assert outcome.eligible_symbols == ["A", "B"]


def test_safe_noop_gate_is_deterministic() -> None:
    gate = SafeNoopGate(name="noop", timeframe="1D")
    outcome = gate.evaluate(["X"])

    assert outcome.decision == GateDecision.PASS
    assert outcome.eligible_symbols == ["X"]
    assert outcome.artifacts.get("status") == "NOT_IMPLEMENTED"
    assert gate.emit_artifacts(["X"]) == {"status": "NOT_IMPLEMENTED"}
