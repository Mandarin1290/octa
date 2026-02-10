from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from octa.core.data.sources.altdata.cache import read_meta, read_snapshot, source_day_dir, write_snapshot
from octa.core.data.sources.altdata.orchestrator import build_altdata_stack, load_altdata_config


def _discover_symbols() -> list[str]:
    return []


def _as_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _audit_path(audit_root: str | None, schedule_ts_utc: datetime) -> Path:
    root = Path(audit_root) if audit_root else Path("octa") / "var" / "audit" / "altdata_refresh"
    root.mkdir(parents=True, exist_ok=True)
    safe_ts = _as_utc(schedule_ts_utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    return root / f"altdata_refresh_{safe_ts}.json"


def _source_lists(cfg: Mapping[str, Any], summary_sources: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    refresh_cfg = cfg.get("refresh", {}) if isinstance(cfg, Mapping) else {}
    required = refresh_cfg.get("required_sources")
    optional = refresh_cfg.get("optional_sources")
    if isinstance(required, list) and isinstance(optional, list):
        return [str(s) for s in required], [str(s) for s in optional]
    sources_cfg = cfg.get("sources", {}) if isinstance(cfg, Mapping) else {}
    required_sources = []
    for name, src_cfg in sources_cfg.items():
        if not isinstance(src_cfg, Mapping):
            continue
        if bool(src_cfg.get("enabled", False)):
            required_sources.append(str(name))
    return required_sources, []


def _cache_diagnostics(sources: Sequence[str], *, asof_date, cache_root: str | None) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {}
    for source in sources:
        meta = read_meta(source=source, asof=asof_date, root=cache_root)
        if isinstance(meta, dict):
            source_meta = meta.get("source_meta")
            if isinstance(source_meta, dict):
                diagnostics[source] = dict(source_meta)
            else:
                diagnostics[source] = {}
        else:
            diagnostics[source] = {}
    return diagnostics


def run_daily_altdata_refresh(
    *,
    schedule_ts_utc: datetime,
    config_path: str | None = None,
    symbols: list[str] | None = None,
    audit_root: str | None = None,
) -> dict[str, Any]:
    schedule_ts_utc = _as_utc(schedule_ts_utc)
    asof_date = schedule_ts_utc.date()
    cfg = load_altdata_config(config_path)
    cache_root = cfg.get("cache_dir") if isinstance(cfg, Mapping) else None
    symbols = list(symbols) if symbols is not None else _discover_symbols()
    run_id = f"altdata_daily_{asof_date.isoformat()}"

    prev_daily = os.environ.get("OCTA_DAILY_REFRESH")
    os.environ["OCTA_DAILY_REFRESH"] = "1"
    try:
        summary = build_altdata_stack(
            run_id=run_id,
            symbols=symbols,
            asof=asof_date,
            allow_net=True,
            config_path=config_path,
        )
    except Exception as exc:
        audit = {
            "run_id": run_id,
            "asof": asof_date.isoformat(),
            "status": "failed",
            "success": False,
            "error": type(exc).__name__,
            "error_message": str(exc)[:200],
        }
        _audit_path(audit_root, schedule_ts_utc).write_text(
            json.dumps(audit, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        return audit
    finally:
        if prev_daily is None:
            os.environ.pop("OCTA_DAILY_REFRESH", None)
        else:
            os.environ["OCTA_DAILY_REFRESH"] = prev_daily

    sources = summary.get("sources", {}) if isinstance(summary, Mapping) else {}
    required, optional = _source_lists(cfg, sources)

    required_missing = [s for s in required if sources.get(s, {}).get("status") != "ok"]
    optional_missing = [s for s in optional if sources.get(s, {}).get("status") != "ok"]

    if required_missing:
        status = "failed"
    elif optional_missing:
        status = "success_with_warnings"
    else:
        status = "success"

    sources_ok = [s for s, v in sources.items() if isinstance(v, Mapping) and v.get("status") == "ok"]
    sources_failed = [s for s, v in sources.items() if not (isinstance(v, Mapping) and v.get("status") == "ok")]

    audit = {
        "run_id": run_id,
        "asof": asof_date.isoformat(),
        "status": status,
        "success": status in {"success", "success_with_warnings"},
        "sources_ok": sources_ok,
        "sources_failed": sources_failed,
        "required_missing": required_missing,
        "optional_missing": optional_missing,
        "sources": sources,
        "cache_diagnostics": _cache_diagnostics(list(sources.keys()), asof_date=asof_date, cache_root=cache_root),
    }

    _audit_path(audit_root, schedule_ts_utc).write_text(
        json.dumps(audit, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return audit


__all__ = [
    "run_daily_altdata_refresh",
    "build_altdata_stack",
    "load_altdata_config",
    "read_snapshot",
    "source_day_dir",
    "write_snapshot",
]
