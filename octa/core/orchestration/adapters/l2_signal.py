from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from octa.core.gates.signal_engine.gate import SignalGate
from octa.core.data.providers.ohlcv import OHLCVProvider


@dataclass
class L2SignalResult:
    symbol: str
    timeframe: str
    decision: str
    reason: str
    payload: Dict[str, Any]


class L2SignalAdapter:
    def __init__(self, provider: OHLCVProvider) -> None:
        self._gate = SignalGate(ohlcv_provider=provider)

    def evaluate(self, *, symbol: str) -> L2SignalResult:
        _ = self._gate.evaluate([symbol])
        payloads = self._gate.emit_artifacts([symbol])
        payload = payloads.get(symbol, {}) if isinstance(payloads, dict) else {}
        decision = payload.get("decision", "FAIL")
        reason = _reason_from_payload(payload)
        return L2SignalResult(
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
    signal = payload.get("signal") if isinstance(payload, dict) else None
    if isinstance(signal, dict):
        if signal.get("direction") == "FLAT":
            return "flat_signal"
        conf = signal.get("confidence")
        if conf is not None and float(conf) <= 0.0:
            return "low_confidence"
    return "gate_fail"

