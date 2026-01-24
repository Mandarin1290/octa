from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from octa.core.data.providers.ohlcv import OHLCVProvider
from octa.core.gates.execution_engine.gate import ExecutionGate


@dataclass
class L4ExecutionResult:
    symbol: str
    timeframe: str
    decision: str
    reason: str
    payload: Dict[str, Any]


class L4ExecutionAdapter:
    def __init__(self, provider: OHLCVProvider, signal_map: Optional[Mapping[str, Mapping[str, Any]]] = None) -> None:
        self._provider = provider
        self._gate = ExecutionGate(ohlcv_provider=provider, signal_map=signal_map or {})

    def set_signal_map(self, signal_map: Mapping[str, Mapping[str, Any]]) -> None:
        self._gate = ExecutionGate(ohlcv_provider=self._provider, signal_map=signal_map)

    def evaluate(self, *, symbol: str) -> L4ExecutionResult:
        _ = self._gate.evaluate([symbol])
        payloads = self._gate.emit_artifacts([symbol])
        payload = payloads.get(symbol, {}) if isinstance(payloads, dict) else {}
        decision = payload.get("decision", "FAIL")
        reason = _reason_from_payload(payload)
        return L4ExecutionResult(
            symbol=symbol,
            timeframe=self._gate.timeframe,
            decision=decision,
            reason=reason,
            payload=payload,
        )


def _reason_from_payload(payload: Dict[str, Any]) -> str:
    flags = payload.get("quality_flags") if isinstance(payload, dict) else None
    if isinstance(flags, dict):
        if flags.get("missing_data"):
            return str(flags.get("reason") or "missing_data")
        if flags.get("gap_risk"):
            return "gap_risk"
        if flags.get("low_liquidity"):
            return "low_liquidity"
    plan = payload.get("execution_plan") if isinstance(payload, dict) else None
    if isinstance(plan, dict) and plan.get("action") != "ENTER":
        return "no_entry"
    return "gate_fail"
