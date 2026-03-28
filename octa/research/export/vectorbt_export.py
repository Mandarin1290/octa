from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


REQUIRED_METADATA_KEYS = ("strategy_name", "timeframe", "params", "source")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _ensure_parquet_engine() -> None:
    if importlib.util.find_spec("pyarrow") or importlib.util.find_spec("fastparquet"):
        return
    raise RuntimeError(
        "parquet export requires 'pyarrow' or 'fastparquet' in the active environment"
    )


def _validate_dataframe(name: str, df: pd.DataFrame) -> None:
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{name} must be a pandas DataFrame")
    if df.empty:
        raise ValueError(f"{name} must not be empty")
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(f"{name} must use a DatetimeIndex")
    if not df.index.is_monotonic_increasing:
        raise ValueError(f"{name} index must be monotonic increasing")


def _validate_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(metadata, Mapping):
        raise TypeError("metadata must be a mapping")
    missing = [key for key in REQUIRED_METADATA_KEYS if key not in metadata]
    if missing:
        raise ValueError(f"metadata missing required keys: {missing}")
    if not isinstance(metadata["params"], Mapping):
        raise TypeError("metadata['params'] must be a mapping")
    normalized = dict(metadata)
    json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return normalized


def export_strategy_outputs(
    df_signals: pd.DataFrame,
    df_returns: pd.DataFrame,
    metadata: Mapping[str, Any],
    out_dir: str | Path,
) -> dict[str, Any]:
    _validate_dataframe("df_signals", df_signals)
    _validate_dataframe("df_returns", df_returns)
    normalized_metadata = _validate_metadata(metadata)
    _ensure_parquet_engine()

    export_dir = Path(out_dir)
    export_dir.mkdir(parents=True, exist_ok=True)

    signals_path = export_dir / "signals.parquet"
    returns_path = export_dir / "returns.parquet"
    metadata_path = export_dir / "metadata.json"
    manifest_path = export_dir / "export_manifest.json"

    for path in (signals_path, returns_path, metadata_path, manifest_path):
        if path.exists():
            raise FileExistsError(f"refusing to overwrite existing artifact: {path}")

    df_signals.to_parquet(signals_path)
    df_returns.to_parquet(returns_path)
    metadata_path.write_text(
        json.dumps(normalized_metadata, sort_keys=True, indent=2),
        encoding="utf-8",
    )

    file_entries = {}
    for path in (signals_path, returns_path, metadata_path):
        stat = path.stat()
        file_entries[path.name] = {
            "path": str(path.resolve()),
            "size_bytes": int(stat.st_size),
            "sha256": _sha256_file(path),
        }

    manifest = {
        "run_id": export_dir.name,
        "timestamp_utc": _utc_now(),
        "python_version": sys.version.split()[0],
        "source_env": {
            "label": Path(sys.prefix).name,
            "prefix": sys.prefix,
            "executable": sys.executable,
        },
        "metadata_keys": list(REQUIRED_METADATA_KEYS),
        "files": file_entries,
        "bundle_sha256": _stable_hash({
            "files": {name: entry["sha256"] for name, entry in sorted(file_entries.items())},
            "run_id": export_dir.name,
            "source_env_prefix": sys.prefix,
        }),
    }
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8")
    return manifest


__all__ = ["REQUIRED_METADATA_KEYS", "export_strategy_outputs"]
