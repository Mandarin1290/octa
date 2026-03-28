from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import yaml

from octa.core.data.sources.altdata.bootstrap_deps import ensure_deps
from octa.core.data.sources.altdata.cache import read_snapshot, write_snapshot
from octa.core.data.sources.altdata.fred import FredSource
from octa.core.data.sources.altdata.edgar import EdgarSource
from octa.core.data.sources.altdata.gdelt import GdeltSource
from octa.core.data.sources.altdata.cot import CotSource
from octa.core.data.sources.altdata.eia import EiaSource
from octa.core.data.sources.altdata.ecb import EcbSource
from octa.core.data.sources.altdata.worldbank import WorldBankSource
from octa.core.data.sources.altdata.oecd import OecdSource
from octa.core.data.sources.altdata.google_trends import GoogleTrendsSource
from octa.core.data.sources.altdata.wikipedia import WikipediaSource
from octa.core.data.sources.altdata.reddit import RedditSource
from octa.core.data.sources.altdata.stooq import StooqSource
from octa.core.data.sources.altdata.fmp import FmpSource
from octa.core.data.sources.altdata.news import NewsEventsSource
from octa.core.data.sources.altdata.news.feed_classifier import recency_model_spec, severity_rules_spec
from octa.core.data.sources.altdata.scheduled_events import ScheduledEventsSource
from octa.core.features.altdata.builders import build_features
from octa.core.features.altdata.registry import FeatureRegistry
from octa.core.features.transforms.feature_builder import (
    AltDataBuildResult,
    build_altdata_features,
)


def load_altdat_config(path: Optional[str] = None) -> Dict[str, Any]:
    p = path or os.getenv("OKTA_ALTDATA_CONFIG") or str(Path("config") / "altdat.yaml")
    try:
        raw = Path(p).read_text()
        cfg = yaml.safe_load(raw) or {}
        if not isinstance(cfg, dict):
            return {}
        return cfg
    except Exception:
        return {}


def run_altdata(
    *,
    bars_df: pd.DataFrame,
    symbol: str,
    tz: str = "UTC",
    config_path: Optional[str] = None,
    asset_class: Optional[str] = None,
    raw_root: Optional[str] = None,
) -> AltDataBuildResult:
    _ = asset_class
    _ = raw_root
    cfg = load_altdat_config(config_path)
    enabled_cfg = cfg.get("enabled", False) if isinstance(cfg, dict) else False
    enabled = bool(enabled_cfg)
    env_enabled = str(os.getenv("OKTA_ALTDATA_ENABLED", "")).strip()
    if env_enabled in {"0", "false", "False"}:
        enabled = False
    elif env_enabled == "1":
        enabled = True
    if not enabled:
        return AltDataBuildResult(features_df=pd.DataFrame(index=bars_df.index), meta={"enabled": False, "status": "DISABLED"})

    auto_install = bool(cfg.get("auto_install", False))
    deps = ensure_deps(auto_install=auto_install)
    if not deps.ok:
        return AltDataBuildResult(
            features_df=pd.DataFrame(index=bars_df.index),
            meta={
                "enabled": False,
                "status": "DEPS_MISSING",
                "missing": deps.missing,
                "attempted_install": deps.attempted_install,
                "errors": deps.errors,
            },
        )

    return build_altdata_features(bars_df=bars_df, symbol=symbol, altdat_cfg=cfg, tz=tz)


def load_altdata_config(path: Optional[str] = None) -> Dict[str, Any]:
    p = path or str(Path("config") / "altdata.yaml")
    try:
        raw = Path(p).read_text()
        cfg = yaml.safe_load(raw) or {}
        if not isinstance(cfg, dict):
            return {}
        return cfg
    except Exception:
        return {}


def build_altdata_stack(
    *,
    run_id: str,
    symbols: list[str],
    asof: Optional[date] = None,
    allow_net: bool = False,
    config_path: Optional[str] = None,
) -> Dict[str, Any]:
    cfg = load_altdata_config(config_path)
    allow_net_effective = _allow_net_effective(allow_net)
    allow_net = allow_net_effective
    if asof is None and _require_asof():
        source_names = _source_names(cfg)
        return {
            "run_id": run_id,
            "asof": None,
            "status": "missing_asof",
            "sources": {name: {"status": "missing_asof"} for name in source_names},
        }
    asof = asof or datetime.now(timezone.utc).date()
    asof_ts = _asof_ts(asof)
    cache_root = cfg.get("cache_dir") if isinstance(cfg, dict) else None
    registry = FeatureRegistry(run_id, root=cache_root)

    sources_cfg = cfg.get("sources", {}) if isinstance(cfg, dict) else {}

    source_instances = _source_instances(sources_cfg)

    summary: Dict[str, Any] = {
        "run_id": run_id,
        "asof": asof.isoformat(),
        "sources": {},
    }

    for source in source_instances:
        if not getattr(source, "enabled", True):
            summary["sources"][source.name] = {"status": "disabled"}
            continue
        payload = read_snapshot(source=source.name, asof=asof, key_suffix=None, root=cache_root)
        fetched_at = datetime.now(timezone.utc).isoformat()
        if payload is None and allow_net:
            raw = source.fetch_raw(asof=asof, allow_net=allow_net)
            if raw is not None:
                payload = source.normalize(raw)
                meta = {
                    "fetched_at": fetched_at,
                    "asof": asof.isoformat(),
                    "source": source.name,
                }
                if isinstance(payload, dict):
                    meta["source_status"] = payload.get("status")
                    meta["source_meta"] = payload.get("meta")
                payload_path, _, h = write_snapshot(
                    source=source.name,
                    asof=asof,
                    payload=payload,
                    meta=meta,
                    root=cache_root,
                )
                registry.write_provenance(
                    source=source.name,
                    asof=asof.isoformat(),
                    fetched_at=fetched_at,
                    content_hash=h,
                    meta={"payload_path": str(payload_path), "meta": meta},
                )
        if payload is None:
            summary["sources"][source.name] = {"status": "missing_cache"}
        else:
            status = "ok"
            if isinstance(payload, dict) and payload.get("status") == "net_error":
                status = "net_error"
            summary["sources"][source.name] = {"status": status, "rows": _payload_rows(payload)}

    # Market-level features (1D)
    payloads_market = {
        k: read_snapshot(source=k, asof=asof, root=cache_root) or {} for k in summary["sources"].keys()
    }
    build_features(
        run_id=run_id,
        timeframe="1D",
        gate_layer="global_1d",
        payloads=payloads_market,
        registry=registry,
        asof_ts=asof_ts,
    )

    # Symbol-level features for Structure/Signal/Execution/Micro
    for symbol in symbols:
        payloads_symbol = payloads_market.copy()
        symbol_sources: Dict[str, Any] = {
            "edgar": EdgarSource({**(sources_cfg.get("edgar") or {}), "symbol": symbol}),
            "reddit": RedditSource({**(sources_cfg.get("reddit") or {}), "symbol": symbol}),
            "google_trends": GoogleTrendsSource({**(sources_cfg.get("google_trends") or {}), "symbol": symbol}),
            "wikipedia": WikipediaSource({**(sources_cfg.get("wikipedia") or {}), "symbol": symbol}),
            "fmp": FmpSource({**(sources_cfg.get("fmp") or {}), "symbol": symbol}),
        }
        for source_name, src in symbol_sources.items():
            payload = read_snapshot(source=source_name, asof=asof, key_suffix=symbol, root=cache_root)
            if payload is None and allow_net and getattr(src, "enabled", True):
                raw = src.fetch_raw(asof=asof, allow_net=allow_net)
                if raw is not None:
                    payload = src.normalize(raw)
                    meta = {
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "asof": asof.isoformat(),
                        "source": source_name,
                        "symbol": symbol,
                    }
                    payload_path, _, h = write_snapshot(
                        source=source_name,
                        asof=asof,
                        payload=payload,
                        meta=meta,
                        key_suffix=symbol,
                        root=cache_root,
                    )
                    registry.write_provenance(
                        source=source_name,
                        asof=asof.isoformat(),
                        fetched_at=meta["fetched_at"],
                        content_hash=h,
                        meta={"payload_path": str(payload_path), "meta": meta},
                    )
            payloads_symbol[source_name] = payload or {}

        build_features(
            run_id=run_id,
            timeframe="30M",
            gate_layer="structure_30m",
            payloads=payloads_symbol,
            registry=registry,
            symbol=symbol,
            asof_ts=asof_ts,
        )
        build_features(
            run_id=run_id,
            timeframe="1H",
            gate_layer="signal_1h",
            payloads=payloads_symbol,
            registry=registry,
            symbol=symbol,
            asof_ts=asof_ts,
        )
        build_features(
            run_id=run_id,
            timeframe="5M",
            gate_layer="execution_5m",
            payloads=payloads_symbol,
            registry=registry,
            symbol=symbol,
            asof_ts=asof_ts,
        )
        build_features(
            run_id=run_id,
            timeframe="1M",
            gate_layer="micro_1m",
            payloads=payloads_symbol,
            registry=registry,
            symbol=symbol,
            asof_ts=asof_ts,
        )

    _write_event_evidence(
        cache_root=cache_root,
        asof=asof,
        summary=summary,
        payloads=payloads_market,
    )

    return summary


def _source_instances(sources_cfg: Dict[str, Any]) -> list[Any]:
    return [
        FredSource(sources_cfg.get("fred") or {}),
        EdgarSource(sources_cfg.get("edgar") or {}),
        GdeltSource(sources_cfg.get("gdelt") or {}),
        CotSource(sources_cfg.get("cot") or {}),
        EiaSource(sources_cfg.get("eia") or {}),
        EcbSource(sources_cfg.get("ecb") or {}),
        WorldBankSource(sources_cfg.get("worldbank") or {}),
        OecdSource(sources_cfg.get("oecd") or {}),
        GoogleTrendsSource(sources_cfg.get("google_trends") or {}),
        WikipediaSource(sources_cfg.get("wikipedia") or {}),
        RedditSource(sources_cfg.get("reddit") or {}),
        StooqSource(sources_cfg.get("stooq") or {}),
        FmpSource(sources_cfg.get("fmp") or {}),
        NewsEventsSource(sources_cfg.get("news_events") or {}),
        ScheduledEventsSource(sources_cfg.get("scheduled_events") or {}),
    ]


def _source_names(cfg: Dict[str, Any]) -> list[str]:
    sources_cfg = cfg.get("sources", {}) if isinstance(cfg, dict) else {}
    return [src.name for src in _source_instances(sources_cfg)]


def _require_asof() -> bool:
    context = str(os.getenv("OCTA_CONTEXT", "")).strip().lower()
    if context in {"backtest", "research", "wfo", "walk_forward", "walk-forward"}:
        return True
    return str(os.getenv("OCTA_REQUIRE_ASOF", "")).strip() == "1"


def _net_disallowed() -> bool:
    context = str(os.getenv("OCTA_CONTEXT", "")).strip().lower()
    return context in {"backtest", "research", "wfo", "walk_forward", "walk-forward"}


def _allow_live_fetch() -> bool:
    if str(os.getenv("OCTA_DAILY_REFRESH", "")).strip() != "1":
        return False
    return not _net_disallowed()


def _allow_net_effective(allow_net: bool) -> bool:
    ctx = str(os.getenv("OCTA_CONTEXT", "")).lower().strip()
    daily = str(os.getenv("OCTA_DAILY_REFRESH", "0")) == "1"
    allow = bool(allow_net)
    if ctx in {"research", "backtest", "wfo"}:
        return False
    return allow and daily


def _asof_ts(asof: date) -> str:
    return datetime.combine(asof, time.min, tzinfo=timezone.utc).isoformat()


def _payload_rows(payload: Any) -> int:
    if isinstance(payload, dict):
        if "series" in payload and isinstance(payload["series"], dict):
            return sum(len(v) for v in payload["series"].values() if isinstance(v, list))
        if "filings" in payload and isinstance(payload["filings"], list):
            return len(payload["filings"])
        if "mappings" in payload and isinstance(payload["mappings"], list):
            return len(payload["mappings"])
        if "rows" in payload and isinstance(payload["rows"], list):
            return len(payload["rows"])
    return 0


def _write_event_evidence(
    *,
    cache_root: str | None,
    asof: date,
    summary: Dict[str, Any],
    payloads: Dict[str, Any],
) -> None:
    root = Path(cache_root) if cache_root else Path("octa") / "var" / "altdata"
    out_dir = root / "evidence" / asof.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)

    scheduled_payload = payloads.get("scheduled_events", {}) if isinstance(payloads, dict) else {}
    scheduled_rows = scheduled_payload.get("rows", []) if isinstance(scheduled_payload, dict) else []
    scheduled_summary = {
        "status": scheduled_payload.get("status", "missing_cache") if isinstance(scheduled_payload, dict) else "missing_cache",
        "event_count": len(scheduled_rows),
        "events": [
            {
                "event_id": row.get("event_id"),
                "scheduled_at": row.get("scheduled_at"),
                "event_type": row.get("event_type"),
                "source_id": row.get("source_id"),
                "source_tier": row.get("source_tier"),
            }
            for row in scheduled_rows
        ],
    }
    scheduled_windows = {
        "events": [
            {
                "event_id": row.get("event_id"),
                "scheduled_at": row.get("scheduled_at"),
                "pre_window_hours": row.get("pre_window_hours"),
                "post_window_hours": row.get("post_window_hours"),
            }
            for row in scheduled_rows
        ]
    }
    manifest = {
        "asof": asof.isoformat(),
        "sources": summary.get("sources", {}),
        "event_layer_sources": ["gdelt", "news_events", "scheduled_events"],
    }

    (out_dir / "recency_model.json").write_text(json.dumps(recency_model_spec(), ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "severity_rules.json").write_text(json.dumps(severity_rules_spec(), ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "scheduled_event_summary.json").write_text(json.dumps(scheduled_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "scheduled_event_windows.json").write_text(json.dumps(scheduled_windows, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "updated_run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
