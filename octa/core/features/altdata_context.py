from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml

from octa.core.data.sources.altdata.cache import read_meta
from octa.core.features.altdata.registry import FeatureRegistry


@dataclass(frozen=True)
class AltDataContext:
    macro_regime_features: dict[str, float]
    event_stress_features: dict[str, float]
    attention_features: dict[str, float]
    positioning_features: dict[str, float]
    sentiment_features: dict[str, float]
    diagnostics: dict[str, Any]
    freshness: dict[str, Any]

    def gate_features(self, gate_layer: str) -> dict[str, float]:
        if gate_layer == "global_1d":
            return {
                **self.macro_regime_features,
                **self.event_stress_features,
                **self.positioning_features,
                **self.attention_features,
            }
        if gate_layer == "signal_1h":
            return {
                **self.event_stress_features,
                **self.sentiment_features,
                **self.attention_features,
            }
        if gate_layer == "structure_30m":
            return {
                **self.event_stress_features,
                **self.positioning_features,
            }
        if gate_layer == "execution_5m":
            return {
                **self.event_stress_features,
                **self.sentiment_features,
            }
        if gate_layer == "micro_1m":
            return {
                **self.attention_features,
                **self.sentiment_features,
            }
        return {}


class AltDataContextBuilder:
    def __init__(
        self,
        *,
        feature_registry: FeatureRegistry | None,
        config: Optional[Mapping[str, Any]] = None,
        cache_root: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> None:
        self._registry = feature_registry
        self._cfg = dict(config or load_altdata_live_config())
        self._cache_root = cache_root
        self._run_id = run_id

    def build(
        self,
        *,
        symbol: Optional[str],
        gate_layer: str,
        timeframe: str,
        asof_ts: Optional[str],
    ) -> AltDataContext:
        market = {}
        symbol_feats = {}
        if self._registry is not None:
            market = self._registry.get_market_feature_vector(
                timeframe="1D",
                gate_layer="global_1d",
                asof_ts=asof_ts,
            )
            if symbol:
                symbol_feats = self._registry.get_feature_vector(
                    symbol=symbol,
                    timeframe=timeframe,
                    gate_layer=gate_layer,
                    asof_ts=asof_ts,
                )

        macro = _filter_macro(market)
        event = _filter_event(market)
        attention = _filter_attention(symbol_feats or market)
        positioning = _filter_positioning(market)
        sentiment = _filter_sentiment(symbol_feats or market)

        asof_date = _asof_date(asof_ts)
        freshness = _source_freshness(self._cfg, asof_date, self._cache_root)
        macro, event, attention, positioning, sentiment, diagnostics = _apply_freshness(
            macro, event, attention, positioning, sentiment, freshness, self._cfg
        )
        diagnostics["gate_layer"] = gate_layer
        diagnostics["timeframe"] = timeframe
        diagnostics["symbol"] = symbol
        diagnostics["asof_ts"] = asof_ts
        diagnostics["run_id"] = self._run_id

        context = AltDataContext(
            macro_regime_features=macro,
            event_stress_features=event,
            attention_features=attention,
            positioning_features=positioning,
            sentiment_features=sentiment,
            diagnostics=diagnostics,
            freshness=freshness,
        )
        _write_audit(context)
        return context


def load_altdata_live_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    path = config_path or os.getenv("OCTA_ALTDATA_LIVE_CONFIG") or str(Path("config") / "altdata_live.yaml")
    try:
        raw = Path(path).read_text()
        cfg = yaml.safe_load(raw) or {}
        return cfg if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def _filter_macro(features: Mapping[str, float]) -> dict[str, float]:
    prefixes = (
        "rates_",
        "curve_",
        "inflation_",
        "energy_shock",
        "ecb_rate",
        "worldbank_",
        "oecd_",
        "macro_risk_score",
        "market_risk_score",
        "proxy_",
    )
    return {k: float(v) for k, v in features.items() if _starts_with(k, prefixes)}


def _filter_event(features: Mapping[str, float]) -> dict[str, float]:
    prefixes = ("event_risk_", "gdelt_", "conflict_", "recession_", "edgar_event_count")
    return {k: float(v) for k, v in features.items() if _starts_with(k, prefixes)}


def _filter_attention(features: Mapping[str, float]) -> dict[str, float]:
    prefixes = ("attention_",)
    return {k: float(v) for k, v in features.items() if _starts_with(k, prefixes)}


def _filter_positioning(features: Mapping[str, float]) -> dict[str, float]:
    prefixes = ("cot_",)
    return {k: float(v) for k, v in features.items() if _starts_with(k, prefixes)}


def _filter_sentiment(features: Mapping[str, float]) -> dict[str, float]:
    prefixes = ("reddit_",)
    return {k: float(v) for k, v in features.items() if _starts_with(k, prefixes)}


def _starts_with(key: str, prefixes: tuple[str, ...]) -> bool:
    return any(str(key).startswith(prefix) for prefix in prefixes)


def _source_freshness(cfg: Mapping[str, Any], asof_date: date, cache_root: Optional[str]) -> Dict[str, Any]:
    sources_cfg = cfg.get("sources", {}) if isinstance(cfg, dict) else {}
    enabled_list = cfg.get("enabled_sources") if isinstance(cfg, dict) else None
    enabled_set = {str(s) for s in enabled_list} if isinstance(enabled_list, list) else None
    freshness: Dict[str, Any] = {"sources": {}, "asof_date": asof_date.isoformat()}
    now = datetime.now(timezone.utc)
    for source, source_cfg in sources_cfg.items():
        if enabled_set is not None and source not in enabled_set:
            continue
        if not isinstance(source_cfg, dict) or not source_cfg.get("enabled", False):
            continue
        ttl = int(source_cfg.get("cache_ttl_seconds", 0) or 0)
        meta = read_meta(source=source, asof=asof_date, root=cache_root)
        fetched_at = _parse_ts(meta.get("fetched_at")) if isinstance(meta, dict) else None
        if fetched_at is None:
            freshness["sources"][source] = {
                "fresh": False,
                "reason": "missing_cache",
                "age_s": None,
            }
            continue
        age_s = (now - fetched_at).total_seconds()
        fresh = True if ttl <= 0 else age_s <= ttl
        freshness["sources"][source] = {
            "fresh": bool(fresh),
            "age_s": age_s,
            "ttl_s": ttl,
            "fetched_at": fetched_at.isoformat(),
        }
    return freshness


def _apply_freshness(
    macro: dict[str, float],
    event: dict[str, float],
    attention: dict[str, float],
    positioning: dict[str, float],
    sentiment: dict[str, float],
    freshness: Dict[str, Any],
    cfg: Mapping[str, Any],
) -> tuple[dict[str, float], dict[str, float], dict[str, float], dict[str, float], dict[str, float], dict[str, Any]]:
    source_map = {
        "macro": ["fred", "ecb", "oecd", "worldbank", "eia", "stooq"],
        "event": ["gdelt"],
        "attention": ["google_trends", "wikipedia"],
        "positioning": ["cot"],
        "sentiment": ["reddit"],
    }
    sources_state = freshness.get("sources", {})
    diagnostics: Dict[str, Any] = {"stale_sources": [], "missing_groups": []}

    def group_fresh(group: str) -> bool:
        sources = source_map.get(group, [])
        if not sources:
            return True
        any_fresh = False
        for src in sources:
            state = sources_state.get(src, {})
            if state.get("fresh"):
                any_fresh = True
            if state.get("fresh") is False:
                diagnostics["stale_sources"].append(src)
        return any_fresh

    if not group_fresh("macro"):
        macro = {}
        diagnostics["missing_groups"].append("macro")
    if not group_fresh("event"):
        event = {}
        diagnostics["missing_groups"].append("event")
    if not group_fresh("attention"):
        attention = {}
        diagnostics["missing_groups"].append("attention")
    if not group_fresh("positioning"):
        positioning = {}
        diagnostics["missing_groups"].append("positioning")
    if not group_fresh("sentiment"):
        sentiment = {}
        diagnostics["missing_groups"].append("sentiment")

    return macro, event, attention, positioning, sentiment, diagnostics


def _write_audit(context: AltDataContext) -> None:
    env_root = os.getenv("OCTA_ALTDATA_LIVE_AUDIT_ROOT")
    root = Path(env_root) if env_root else Path("octa") / "var" / "audit" / "altdata_live"
    root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    safe_ts = ts.replace(":", "").replace("-", "").replace(".", "")
    payload = {
        "timestamp": ts,
        "diagnostics": context.diagnostics,
        "freshness": context.freshness,
        "macro_keys": list(context.macro_regime_features.keys()),
        "event_keys": list(context.event_stress_features.keys()),
        "attention_keys": list(context.attention_features.keys()),
        "positioning_keys": list(context.positioning_features.keys()),
        "sentiment_keys": list(context.sentiment_features.keys()),
    }
    path = root / f"altdata_live_{safe_ts}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _asof_date(asof_ts: Optional[str]) -> date:
    if not asof_ts:
        return datetime.now(timezone.utc).date()
    try:
        dt = datetime.fromisoformat(str(asof_ts))
    except Exception:
        return datetime.now(timezone.utc).date()
    return dt.date()
