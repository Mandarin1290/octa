from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, MutableMapping, Sequence

from .contracts import GateDecision


@dataclass
class CascadeContext:
    symbols_by_gate: MutableMapping[str, list[str]] = field(default_factory=dict)
    decisions: MutableMapping[str, MutableMapping[str, GateDecision]] = field(
        default_factory=dict
    )
    artifacts: MutableMapping[str, MutableMapping[str, Mapping[str, Any]]] = field(
        default_factory=dict
    )
    rejected: MutableMapping[str, list[str]] = field(default_factory=dict)
    trace: list[str] = field(default_factory=list)

    def record_gate_start(self, gate_name: str, symbols: Sequence[str]) -> None:
        self.symbols_by_gate[gate_name] = list(symbols)
        self.trace.append(f"gate_start:{gate_name}:{len(symbols)}")

    def record_gate_decision(
        self, gate_name: str, symbols: Sequence[str], decision: GateDecision
    ) -> None:
        decisions_for_gate = self.decisions.setdefault(gate_name, {})
        for symbol in symbols:
            decisions_for_gate[symbol] = decision
        self.trace.append(f"gate_decision:{gate_name}:{decision.value}")

    def record_gate_artifacts(
        self, gate_name: str, symbols: Sequence[str], artifacts: Mapping[str, Any]
    ) -> None:
        artifacts_for_gate = self.artifacts.setdefault(gate_name, {})
        for symbol in symbols:
            artifacts_for_gate[symbol] = dict(artifacts)
        self.trace.append(f"gate_artifacts:{gate_name}:{len(artifacts)}")

    def record_rejected(self, gate_name: str, symbols: Sequence[str]) -> None:
        self.rejected[gate_name] = list(symbols)
        self.trace.append(f"gate_rejected:{gate_name}:{len(symbols)}")
