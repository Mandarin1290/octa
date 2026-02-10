from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from octa.core.data.sources.altdata.cache import read_snapshot
from octa.core.data.sources.altdata.orchestrator import build_altdata_stack, load_altdata_config
import json
from pathlib import Path


def run_daily_altdata_refresh(
    *,
    now: datetime,
    symbols: list[str] | None = None,
    audit_root: str | None = None,
    config_path: str | None = None,
) -> dict[str, Any]:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    asof_date = now.date()
    cfg = load_altdata_config(config_path)
    cache_root = cfg.get("cache_dir") if isinstance(cfg, dict) else None
    run_id = f"altdata_daily_{asof_date.isoformat()}"
    try:
        summary = build_altdata_stack(
            run_id=run_id,
            symbols=list(symbols or []),
            asof=asof_date,
            allow_net=True,
            config_path=config_path,
        )
        sources = summary.get("sources", {}) if isinstance(summary, dict) else {}
        sources_ok = [s for s, v in sources.items() if isinstance(v, dict) and v.get("status") == "ok"]
        audit = {
            "run_id": run_id,
            "asof": asof_date.isoformat(),
            "success": True,
            "sources_ok": sources_ok,
            "sources": sources,
            "cache_root": cache_root,
        }
    except Exception as exc:
        audit = {
            "run_id": run_id,
            "asof": asof_date.isoformat(),
            "success": False,
            "error": type(exc).__name__,
            "error_message": str(exc)[:200],
            "cache_root": cache_root,
        }
    if audit_root:
        root = Path(audit_root)
        root.mkdir(parents=True, exist_ok=True)
        safe_ts = now.strftime("%Y-%m-%dT%H-%M-%SZ")
        path = root / f"altdata_refresh_{safe_ts}.json"
        path.write_text(json.dumps(audit, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return audit


__all__ = [
    "run_daily_altdata_refresh",
    "build_altdata_stack",
    "load_altdata_config",
    "read_snapshot",
]
