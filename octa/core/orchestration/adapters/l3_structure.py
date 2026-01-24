from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from octa.core.data.providers.ohlcv import OHLCVProvider
from octa.core.gates.structure_filter.gate import StructureGate


@dataclass
class L3StructureResult:
    symbol: str
    timeframe: str
    decision: str
    reason: str
    payload: Dict[str, Any]


class L3StructureAdapter:
    def __init__(self, provider: OHLCVProvider) -> None:
        self._gate = StructureGate(ohlcv_provider=provider)

    def evaluate(self, *, symbol: str) -> L3StructureResult:
        _ = self._gate.evaluate([symbol])
        payloads = self._gate.emit_artifacts([symbol])
        payload = payloads.get(symbol, {}) if isinstance(payloads, dict) else {}
        decision = payload.get("decision", "FAIL")
        reason = _reason_from_payload(payload)
        return L3StructureResult(
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
    return "gate_fail"

