from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from octa.core.cascade.context import CascadeContext
from octa.core.data.providers.ohlcv import OHLCVProvider
from octa.core.gates.micro_optimization.gate import MicroGate


@dataclass
class L5MicroResult:
    symbol: str
    timeframe: str
    decision: str
    reason: str
    payload: Dict[str, Any]


class L5MicroAdapter:
    def __init__(self, provider: OHLCVProvider, execution_map: Optional[Mapping[str, Mapping[str, Any]]] = None) -> None:
        self._provider = provider
        self._gate = MicroGate(ohlcv_provider=provider)
        self._execution_map = dict(execution_map or {})

    def set_execution_map(self, execution_map: Mapping[str, Mapping[str, Any]]) -> None:
        self._execution_map = dict(execution_map)

    def evaluate(self, *, symbol: str) -> L5MicroResult:
        ctx = CascadeContext()
        ctx.artifacts["execution"] = dict(self._execution_map)
        self._gate.set_context(ctx)
        _ = self._gate.evaluate([symbol])
        payloads = self._gate.emit_artifacts([symbol])
        payload = payloads.get(symbol, {}) if isinstance(payloads, dict) else {}
        decision = payload.get("decision", "FAIL")
        reason = _reason_from_payload(payload)
        return L5MicroResult(
            symbol=symbol,
            timeframe=self._gate.timeframe,
            decision=decision,
            reason=reason,
            payload=payload,
        )


def _reason_from_payload(payload: Dict[str, Any]) -> str:
    flags = payload.get("micro_risk_flags") if isinstance(payload, dict) else None
    if isinstance(flags, dict):
        if flags.get("reason"):
            return str(flags.get("reason"))
        if flags.get("gap_risk"):
            return "gap_risk"
        if flags.get("liquidity_thin"):
            return "liquidity_thin"
    return "gate_fail"
