from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from .contracts import GateDecision, GateOutcome, GateInterface

logger = logging.getLogger(__name__)


@runtime_checkable
class GateDelegate(Protocol):
    name: str
    timeframe: str

    def fit(self, symbols: Sequence[str]) -> None:
        ...

    def evaluate(self, symbols: Sequence[str]) -> Any:
        ...

    def filter_universe(self, symbols: Sequence[str]) -> Sequence[str]:
        ...

    def emit_artifacts(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        ...


def _coerce_decision(raw: Any) -> GateDecision:
    if isinstance(raw, GateDecision):
        return raw
    if isinstance(raw, str):
        upper = raw.upper()
        if upper in (GateDecision.PASS.value, GateDecision.FAIL.value):
            return GateDecision(upper)
    if isinstance(raw, bool):
        return GateDecision.PASS if raw else GateDecision.FAIL
    raise TypeError(f"Unsupported gate decision: {raw!r}")


def _coerce_outcome(raw: Any, symbols: Sequence[str]) -> GateOutcome:
    if isinstance(raw, GateOutcome):
        return raw
    decision = _coerce_decision(raw)
    eligible = list(symbols) if decision == GateDecision.PASS else []
    rejected = [] if decision == GateDecision.PASS else list(symbols)
    return GateOutcome(decision=decision, eligible_symbols=eligible, rejected_symbols=rejected)


@dataclass(kw_only=True)
class BaseGateAdapter(GateInterface):
    name: str
    timeframe: str
    delegate: GateDelegate

    def fit(self, symbols: Sequence[str]) -> None:
        self.delegate.fit(symbols)

    def evaluate(self, symbols: Sequence[str]) -> GateOutcome:
        raw = self.delegate.evaluate(symbols)
        return _coerce_outcome(raw, symbols)

    def filter_universe(self, symbols: Sequence[str]) -> Sequence[str]:
        return self.delegate.filter_universe(symbols)

    def emit_artifacts(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        return self.delegate.emit_artifacts(symbols)


@dataclass
class GlobalRegimeGateAdapter(BaseGateAdapter):
    name: str = "global_regime"
    timeframe: str = "1D"


@dataclass
class StructureGateAdapter(BaseGateAdapter):
    name: str = "structure"
    timeframe: str = "30M"


@dataclass
class SignalGateAdapter(BaseGateAdapter):
    name: str = "signal"
    timeframe: str = "1H"


@dataclass
class ExecutionGateAdapter(BaseGateAdapter):
    name: str = "execution"
    timeframe: str = "5M"


@dataclass
class MicroGateAdapter(BaseGateAdapter):
    name: str = "micro"
    timeframe: str = "1M"


@dataclass
class SafeNoopGate(GateInterface):
    name: str
    timeframe: str

    def fit(self, symbols: Sequence[str]) -> None:
        logger.info("cascade_gate_noop", extra={"gate": self.name, "count": len(symbols)})

    def evaluate(self, symbols: Sequence[str]) -> GateOutcome:
        logger.info("cascade_gate_noop", extra={"gate": self.name, "count": len(symbols)})
        return GateOutcome(
            decision=GateDecision.PASS,
            eligible_symbols=list(symbols),
            rejected_symbols=[],
            artifacts={"status": "NOT_IMPLEMENTED"},
        )

    def filter_universe(self, symbols: Sequence[str]) -> Sequence[str]:
        return list(symbols)

    def emit_artifacts(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        return {"status": "NOT_IMPLEMENTED"}
