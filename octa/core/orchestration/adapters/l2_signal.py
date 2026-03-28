from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Sequence

from octa.core.gates.signal_engine.gate import SignalGate
from octa.core.data.providers.ohlcv import OHLCVBar, OHLCVProvider, Timeframe
from octa.core.features.altdata.registry import FeatureRegistry


@dataclass
class L2SignalResult:
    symbol: str
    timeframe: Timeframe
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
        overlay = _apply_altdata_overlay(
            altdata_market,
            config=_resolve_overlays(self._altdata_config),
            gate_layer="signal_1h",
        )
        if overlay:
            payload["altdata_overlay"] = overlay
            if overlay.get("block_new_entries"):
                decision = "FAIL"
                payload["decision"] = decision
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
    timeframe: Timeframe,
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


def _resolve_overlays(config: Mapping[str, Any]) -> Mapping[str, Any]:
    if "overlays" in config:
        overlays = config.get("overlays")
        return overlays if isinstance(overlays, Mapping) else {}
    if "gate_overlays" in config:
        overlays = config.get("gate_overlays")
        return overlays if isinstance(overlays, Mapping) else {}
    return {}


def _apply_altdata_overlay(
    altdata_market: Mapping[str, float],
    *,
    config: Mapping[str, Any],
    gate_layer: str,
) -> Dict[str, Any] | None:
    if not isinstance(config, Mapping):
        return None
    if config.get("enabled") is False:
        return None
    gate_cfg = config.get(gate_layer)
    if not isinstance(gate_cfg, Mapping):
        return None
    if gate_cfg.get("enabled") is False:
        return None

    overlay: Dict[str, Any] = {}
    news_critical = float(altdata_market.get("news_critical_flag", 0.0) or 0.0) >= 1.0
    news_official = float(altdata_market.get("news_official_flag", 0.0) or 0.0) >= 1.0
    if news_critical and news_official:
        overlay["block_new_entries"] = True
        overlay["reason"] = "altdata_news_critical_block"
        return overlay

    risk_limit = gate_cfg.get("event_risk_max")
    news_risk_score = float(altdata_market.get("news_risk_score", 0.0) or 0.0)
    if risk_limit is not None and news_risk_score > float(risk_limit):
        overlay["block_new_entries"] = True
        overlay["reason"] = "altdata_news_risk_high"
        return overlay

    position_size_multiplier = 1.0
    max_exposure_scale = 1.0
    risk_multiplier = 1.0

    if (
        float(altdata_market.get("news_central_bank_flag", 0.0) or 0.0) >= 1.0
        or float(altdata_market.get("scheduled_macro_window_flag", 0.0) or 0.0) >= 1.0
    ):
        position_size_multiplier = min(position_size_multiplier, 0.5)
        max_exposure_scale = min(max_exposure_scale, 0.5)
        risk_multiplier = max(risk_multiplier, 1.25)
        overlay["reason"] = "altdata_central_bank_window"

    if (
        float(altdata_market.get("news_energy_flag", 0.0) or 0.0) >= 1.0
        or float(altdata_market.get("news_geopolitics_flag", 0.0) or 0.0) >= 1.0
    ):
        position_size_multiplier = min(position_size_multiplier, 0.75)
        max_exposure_scale = min(max_exposure_scale, 0.7)
        risk_multiplier = max(risk_multiplier, 1.4)
        overlay["reason"] = overlay.get("reason") or "altdata_energy_geopolitics_risk"

    news_tier3_count = float(altdata_market.get("news_tier3_count", 0.0) or 0.0)
    news_tier1_count = float(altdata_market.get("news_tier1_count", 0.0) or 0.0)
    if news_tier3_count >= 3.0 and news_tier1_count <= 0.0:
        position_size_multiplier = min(position_size_multiplier, 0.9)
        max_exposure_scale = min(max_exposure_scale, 0.95)
        risk_multiplier = max(risk_multiplier, 1.05)
        overlay["reason"] = overlay.get("reason") or "altdata_tier3_noise"

    if position_size_multiplier >= 1.0 and max_exposure_scale >= 1.0 and risk_multiplier <= 1.0:
        return None

    overlay["position_size_multiplier"] = position_size_multiplier
    overlay["max_exposure_scale"] = max_exposure_scale
    overlay["risk_multiplier"] = risk_multiplier
    overlay["block_new_entries"] = False
    return overlay
