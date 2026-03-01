"""AltDataSnapshotRegistry — canonical snapshot manifest for reproducible training.

A snapshot manifest records exactly which local cache files were used for a training
run: source name, cache date (may be < asof due to nearest-prior fallback), sha256
hash, and row count. Written once per asof date; read back by training to verify
data provenance and reproduce feature inputs.

Snapshot-ID format: YYYYMMDDThhmmssZ  (daily snapshots use T000000Z)
Manifest location:  octa/var/altdata_snapshots/<snapshot_id>/manifest.json

Design constraints:
- Pure local I/O — no network calls.
- Fail-soft: missing source cache → {"status": "missing"} in manifest, no exception.
- Idempotent: resolve_and_write() returns existing manifest path if already present.
- No changes to cascade, pipeline, or gate logic.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULT_CACHE_ROOT = Path("octa") / "var" / "altdata"
_DEFAULT_SNAPSHOTS_ROOT = Path("octa") / "var" / "altdata_snapshots"

# Sources whose per-day cache dirs follow the layout:
#   <cache_root>/<source>/<YYYY-MM-DD>/<source>_<YYYY-MM-DD>.json
_KNOWN_MARKET_SOURCES = [
    "fred", "gdelt", "stooq", "cot", "eia", "ecb",
    "worldbank", "oecd", "google_trends", "wikipedia",
    "nasa_firms", "viirs_nightlights",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_cache_root(cache_root: Optional[str]) -> Path:
    return Path(cache_root) if cache_root else _DEFAULT_CACHE_ROOT


def _resolve_snapshots_root(snapshots_root: Optional[str]) -> Path:
    return Path(snapshots_root) if snapshots_root else _DEFAULT_SNAPSHOTS_ROOT


def _snapshot_id_from_date(asof: date) -> str:
    return asof.strftime("%Y%m%d") + "T000000Z"


def _find_nearest_prior_date(source: str, target: date, cache_root: Path) -> Optional[date]:
    """Return the latest cached date <= target for a given source, or None."""
    src_dir = cache_root / source
    if not src_dir.is_dir():
        return None
    best: Optional[date] = None
    for entry in src_dir.iterdir():
        if not entry.is_dir():
            continue
        try:
            d = date.fromisoformat(entry.name)
        except (ValueError, TypeError):
            continue
        if d <= target and (best is None or d > best):
            best = d
    return best


def _payload_path(source: str, cache_date: date, cache_root: Path) -> Path:
    return cache_root / source / cache_date.isoformat() / f"{source}_{cache_date.isoformat()}.json"


def _sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _count_rows(payload: Dict[str, Any]) -> int:
    """Best-effort row count from a snapshot payload dict."""
    if "series" in payload and isinstance(payload["series"], dict):
        return sum(len(v) for v in payload["series"].values() if isinstance(v, list))
    if "rows" in payload and isinstance(payload["rows"], list):
        return len(payload["rows"])
    if "filings" in payload and isinstance(payload["filings"], list):
        return len(payload["filings"])
    if "mappings" in payload and isinstance(payload["mappings"], list):
        return len(payload["mappings"])
    return len(payload) if isinstance(payload, dict) else 0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_and_write(
    asof: date,
    *,
    cache_root: Optional[str] = None,
    snapshots_root: Optional[str] = None,
    sources: Optional[List[str]] = None,
) -> Tuple[str, Path]:
    """Resolve the best available cache snapshot for each source and write a manifest.

    Returns (snapshot_id, manifest_path).
    Idempotent: if manifest already exists, returns immediately without overwriting.

    Args:
        asof: The target date for feature data (training end date or refresh date).
        cache_root: Override for the altdata cache root (default: octa/var/altdata).
        snapshots_root: Override for manifest storage root (default: octa/var/altdata_snapshots).
        sources: List of source names to include (default: all known market sources).
    """
    cr = _resolve_cache_root(cache_root)
    sr = _resolve_snapshots_root(snapshots_root)
    src_list = sources if sources is not None else _KNOWN_MARKET_SOURCES
    snapshot_id = _snapshot_id_from_date(asof)
    manifest_path = sr / snapshot_id / "manifest.json"

    if manifest_path.exists():
        return snapshot_id, manifest_path

    created_at = datetime.now(timezone.utc).isoformat()
    source_records: Dict[str, Any] = {}
    ok_count = 0
    missing_count = 0

    for src in src_list:
        cache_date = _find_nearest_prior_date(src, asof, cr)
        if cache_date is None:
            source_records[src] = {"status": "missing", "cache_date": None}
            missing_count += 1
            continue

        p = _payload_path(src, cache_date, cr)
        if not p.exists():
            source_records[src] = {"status": "missing", "cache_date": cache_date.isoformat()}
            missing_count += 1
            continue

        try:
            sha = _sha256_file(p)
            payload = json.loads(p.read_text(encoding="utf-8"))
            rows = _count_rows(payload)
            stale = (asof - cache_date).days
            source_records[src] = {
                "status": "ok",
                "cache_date": cache_date.isoformat(),
                "stale_days": stale,
                "payload_path": str(p),
                "sha256": sha,
                "row_count": rows,
            }
            ok_count += 1
        except Exception as exc:
            source_records[src] = {
                "status": "error",
                "cache_date": cache_date.isoformat(),
                "error": str(exc),
            }
            missing_count += 1

    manifest: Dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "asof_date": asof.isoformat(),
        "created_at": created_at,
        "sources": source_records,
        "total_sources_ok": ok_count,
        "total_sources_missing": missing_count,
    }

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = manifest_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(manifest_path)

    return snapshot_id, manifest_path


def read_manifest(
    snapshot_id: str,
    *,
    snapshots_root: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Read a previously written manifest by snapshot_id. Returns None if not found."""
    sr = _resolve_snapshots_root(snapshots_root)
    p = sr / snapshot_id / "manifest.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def list_available_snapshots(
    *,
    snapshots_root: Optional[str] = None,
) -> List[str]:
    """Return sorted list of all snapshot_ids present in the snapshots root."""
    sr = _resolve_snapshots_root(snapshots_root)
    if not sr.is_dir():
        return []
    ids = []
    for entry in sr.iterdir():
        if entry.is_dir() and (entry / "manifest.json").exists():
            ids.append(entry.name)
    return sorted(ids)


def get_latest_snapshot_id(
    *,
    snapshots_root: Optional[str] = None,
) -> Optional[str]:
    """Return the most recent snapshot_id, or None if no snapshots exist."""
    ids = list_available_snapshots(snapshots_root=snapshots_root)
    return ids[-1] if ids else None
