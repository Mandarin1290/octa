from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class CachePaths:
    root: Path


def resolve_cache_root(root: str | None = None) -> Path:
    base = Path(root) if root else Path("octa") / "var" / "altdata"
    return base


def source_day_dir(source: str, asof: date, root: str | None = None) -> Path:
    base = resolve_cache_root(root)
    return base / source / asof.isoformat()


def cache_key(source: str, asof: date, key_suffix: str | None = None) -> str:
    base = f"{source}_{asof.isoformat()}"
    return f"{base}_{key_suffix}" if key_suffix else base


def write_snapshot(
    *,
    source: str,
    asof: date,
    payload: Mapping[str, Any],
    meta: Mapping[str, Any],
    root: str | None = None,
    key_suffix: str | None = None,
) -> tuple[Path, Path, str]:
    out_dir = source_day_dir(source, asof, root)
    out_dir.mkdir(parents=True, exist_ok=True)

    key = cache_key(source, asof, key_suffix)
    payload_path = out_dir / f"{key}.json"
    meta_path = out_dir / f"{key}_meta.json"

    raw = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    tmp_payload = payload_path.with_suffix(".json.tmp")
    tmp_meta = meta_path.with_suffix(".json.tmp")

    tmp_payload.write_text(raw, encoding="utf-8")
    tmp_payload.replace(payload_path)
    tmp_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp_meta.replace(meta_path)

    return payload_path, meta_path, h


def read_snapshot(
    *,
    source: str,
    asof: date,
    root: str | None = None,
    key_suffix: str | None = None,
) -> Optional[dict[str, Any]]:
    key = cache_key(source, asof, key_suffix)
    payload_path = source_day_dir(source, asof, root) / f"{key}.json"
    if not payload_path.exists():
        return None
    try:
        return json.loads(payload_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def read_meta(
    *,
    source: str,
    asof: date,
    root: str | None = None,
    key_suffix: str | None = None,
) -> Optional[dict[str, Any]]:
    key = cache_key(source, asof, key_suffix)
    meta_path = source_day_dir(source, asof, root) / f"{key}_meta.json"
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
