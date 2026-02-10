from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Sequence

from octa.core.gates.signal_engine.gate import SignalGate
from octa.core.data.providers.ohlcv import OHLCVBar, OHLCVProvider
from octa.core.features.altdata.registry import FeatureRegistry


@dataclass
class L2SignalResult:
    symbol: str
    timeframe: str
    decision: str
    reason: str
    payload: Dict[str, Any]


class L2SignalAdapter:
    def __init__(
        self,
        provider: OHLCVProvider,
        *,
        feature_registry: FeatureRegistry | None = None,
        altdata_config: Mapping[str, Any] | None = None,
    ) -> None:
        self._provider = provider
        self._gate = SignalGate(ohlcv_provider=provider)
        self._feature_registry = feature_registry
        self._altdata_config = dict(altdata_config or {})

    def evaluate(self, *, symbol: str) -> L2SignalResult:
        _ = self._gate.evaluate([symbol])
        payloads = self._gate.emit_artifacts([symbol])
        payload = payloads.get(symbol, {}) if isinstance(payloads, dict) else {}
        payload = dict(payload) if isinstance(payload, dict) else {}
        bars = self._provider.get_ohlcv(symbol, self._gate.timeframe)
        asof_ts = _resolve_asof_ts(payload, bars)
        altdata_symbol, altdata_market = _load_altdata(
            self._feature_registry,
            symbol=symbol,
            timeframe=self._gate.timeframe,
            gate_layer="signal_1h",
            asof_ts=asof_ts,
        )
        payload["altdata_symbol"] = altdata_symbol
        payload["altdata_market"] = altdata_market
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
    overlay = payload.get("altdata_overlay") if isinstance(payload, dict) else None
    if isinstance(overlay, dict):
        reason = overlay.get("reason")
        if reason:
            return str(reason)
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


def _latest_bar_ts(bars: Sequence[OHLCVBar]) -> str | None:
    if not bars:
        return None
    ts = bars[-1].ts
    return _normalize_ts(ts)


def _resolve_asof_ts(payload: Mapping[str, Any], bars: Sequence[OHLCVBar]) -> str | None:
    for key in ("asof_ts", "ts", "bar_ts", "timestamp"):
        if key in payload:
            normalized = _normalize_ts(payload.get(key))
            if normalized is not None:
                return normalized
    return _latest_bar_ts(bars)


def _normalize_ts(value: Any) -> str | None:
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        try:
            ts = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()


def _load_altdata(
    registry: FeatureRegistry | None,
    *,
    symbol: str,
    timeframe: str,
    gate_layer: str,
    asof_ts: str | None,
) -> tuple[Dict[str, float], Dict[str, float]]:
    if registry is None:
        return {}, {}
    symbol_feats = registry.get_feature_vector(
        symbol=symbol,
        timeframe=timeframe,
        gate_layer=gate_layer,
        asof_ts=asof_ts,
    )
    market_feats = registry.get_market_feature_vector(
        timeframe="1D",
        gate_layer="global_1d",
        asof_ts=asof_ts,
    )
    return symbol_feats, market_feats
