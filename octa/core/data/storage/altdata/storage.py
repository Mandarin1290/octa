from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


@dataclass
class StoragePaths:
    root: Path
    parquet_dir: Path
    meta_dir: Path
    cache_dir: Path
    db_path: Path


def resolve_storage_root(*, cfg_root: Optional[str] = None) -> Path:
    env_root = str(os.getenv("OKTA_ALTDATA_ROOT", "")).strip()
    if env_root:
        return Path(env_root)
    if cfg_root:
        return Path(str(cfg_root))
    return Path("data") / "altdat"


def make_paths(*, cfg_root: Optional[str] = None) -> StoragePaths:
    root = resolve_storage_root(cfg_root=cfg_root)
    parquet_dir = root / "parquet"
    meta_dir = root / "meta"
    cache_dir = root / "cache"
    db_path = root / "altdat.duckdb"
    return StoragePaths(root=root, parquet_dir=parquet_dir, meta_dir=meta_dir, cache_dir=cache_dir, db_path=db_path)


def ensure_dirs(paths: StoragePaths) -> None:
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.parquet_dir.mkdir(parents=True, exist_ok=True)
    paths.meta_dir.mkdir(parents=True, exist_ok=True)
    paths.cache_dir.mkdir(parents=True, exist_ok=True)


DUCKDB_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta_runs (
  run_id VARCHAR,
  created_at TIMESTAMP,
  git_hash VARCHAR,
  config_hash VARCHAR,
  notes VARCHAR
);

CREATE TABLE IF NOT EXISTS meta_sources (
  source VARCHAR,
  version VARCHAR,
  last_ok TIMESTAMP,
  last_error VARCHAR,
  rate_limit_state VARCHAR
);

CREATE TABLE IF NOT EXISTS fred_series (
  series_id VARCHAR,
  ts TIMESTAMP,
  value DOUBLE,
  as_of TIMESTAMP,
  source_time TIMESTAMP,
  ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS edgar_filings (
  cik VARCHAR,
  ticker VARCHAR,
  form VARCHAR,
  filing_date DATE,
  accepted_datetime TIMESTAMP,
  doc_url VARCHAR,
  raw_text_path VARCHAR,
  as_of TIMESTAMP,
  ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS feature_store (
  symbol VARCHAR,
  timeframe VARCHAR,
  ts TIMESTAMP,
  feature_name VARCHAR,
  value DOUBLE,
  as_of TIMESTAMP,
  provenance_json VARCHAR
);
"""


def init_duckdb(paths: StoragePaths) -> Tuple[bool, Optional[str]]:
    """Initialize DuckDB schema.

    Returns (ok, error).
    """
    try:
        import duckdb  # type: ignore
    except Exception as e:
        return False, f"duckdb import failed: {e}"

    try:
        ensure_dirs(paths)
        con = duckdb.connect(str(paths.db_path))
        con.execute(DUCKDB_SCHEMA_SQL)
        con.close()
        return True, None
    except Exception as e:
        return False, f"duckdb init failed: {e}"


def write_meta_json(paths: StoragePaths, name: str, payload: Dict[str, Any]) -> None:
    ensure_dirs(paths)
    out = paths.meta_dir / name
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def utcnow_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"
