from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from typing import Any

import pandas as pd


REQUIRED_EXPORT_FILES = (
    "signals.parquet",
    "returns.parquet",
    "metadata.json",
    "export_manifest.json",
)
REQUIRED_METADATA_KEYS = ("strategy_name", "timeframe", "params", "source")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON in {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def _ensure_parquet_engine() -> None:
    if importlib.util.find_spec("pyarrow") or importlib.util.find_spec("fastparquet"):
        return
    raise RuntimeError(
        "parquet import requires 'pyarrow' or 'fastparquet' in the active environment"
    )


def _validate_manifest(export_dir: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise ValueError("export_manifest.json missing files map")

    file_hashes: dict[str, str] = {}
    for name in REQUIRED_EXPORT_FILES[:-1]:
        if name not in files:
            raise ValueError(f"export_manifest.json missing entry for {name}")
        entry = files[name]
        if not isinstance(entry, dict):
            raise ValueError(f"invalid manifest entry for {name}")
        declared_path = entry.get("path")
        declared_hash = entry.get("sha256")
        if not isinstance(declared_path, str) or not isinstance(declared_hash, str):
            raise ValueError(f"invalid path/hash metadata for {name}")
        expected_path = (export_dir / name).resolve()
        if Path(declared_path).resolve() != expected_path:
            raise ValueError(f"MANIFEST_PATH_MISMATCH for {name}")
        actual_hash = _sha256_file(expected_path)
        if actual_hash != declared_hash:
            raise ValueError(f"SHA256_MISMATCH for {name}")
        file_hashes[name] = actual_hash

    declared_bundle = manifest.get("bundle_sha256")
    if not isinstance(declared_bundle, str):
        raise ValueError("export_manifest.json missing bundle_sha256")
    canonical = json.dumps(
        {
            "files": {name: file_hashes[name] for name in sorted(file_hashes)},
            "run_id": export_dir.name,
            "source_env_prefix": manifest.get("source_env", {}).get("prefix"),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    actual_bundle = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if actual_bundle != declared_bundle:
        raise ValueError("BUNDLE_SHA256_MISMATCH")
    return file_hashes


def _validate_metadata(metadata: dict[str, Any]) -> None:
    missing = [key for key in REQUIRED_METADATA_KEYS if key not in metadata]
    if missing:
        raise ValueError(f"metadata missing required keys: {missing}")
    if not isinstance(metadata.get("params"), dict):
        raise TypeError("metadata['params'] must be a dict")


def load_research_export(path: str | Path) -> dict[str, Any]:
    _ensure_parquet_engine()
    export_dir = Path(path)
    if not export_dir.exists():
        raise FileNotFoundError(f"research export path does not exist: {export_dir}")
    if not export_dir.is_dir():
        raise NotADirectoryError(f"research export path is not a directory: {export_dir}")

    for name in REQUIRED_EXPORT_FILES:
        artifact_path = export_dir / name
        if not artifact_path.exists():
            raise FileNotFoundError(f"missing required export artifact: {artifact_path}")

    manifest = _load_json(export_dir / "export_manifest.json")
    _validate_manifest(export_dir, manifest)

    metadata = _load_json(export_dir / "metadata.json")
    _validate_metadata(metadata)

    signals = pd.read_parquet(export_dir / "signals.parquet")
    returns = pd.read_parquet(export_dir / "returns.parquet")
    if not isinstance(signals.index, pd.DatetimeIndex):
        raise TypeError("signals.parquet must load with a DatetimeIndex")
    if not isinstance(returns.index, pd.DatetimeIndex):
        raise TypeError("returns.parquet must load with a DatetimeIndex")

    return {
        "signals": signals,
        "returns": returns,
        "metadata": metadata,
    }


__all__ = ["REQUIRED_EXPORT_FILES", "load_research_export"]
