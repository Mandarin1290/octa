from __future__ import annotations

import hashlib
import json
import platform
import subprocess
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def canonical_json_bytes(payload: Any) -> bytes:
    def _default(value: Any) -> Any:
        if is_dataclass(value):
            return asdict(value)
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, set):
            return sorted(value)
        raise TypeError(f"unsupported type for canonical JSON: {type(value)!r}")

    return json.dumps(
        payload,
        default=_default,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def sha256_file(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(payload))


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def read_yaml(path: Path) -> dict[str, Any]:
    import yaml

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"policy/config must be a mapping: {path}")
    return raw


def environment_snapshot() -> dict[str, Any]:
    # pid intentionally excluded: non-deterministic across processes
    return {
        "cwd": str(Path.cwd()),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
    }


def git_snapshot() -> dict[str, Any]:
    def _run(args: list[str]) -> str:
        try:
            out = subprocess.run(
                args,
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception as exc:
            return f"ERROR:{type(exc).__name__}:{exc}"
        return (out.stdout or out.stderr).strip()

    return {
        "rev_parse_head": _run(["git", "rev-parse", "HEAD"]),
        "status_porcelain": _run(["git", "status", "--short"]),
        "diff_stat": _run(["git", "diff", "--stat"]),
    }


def ensure_relative_to(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path.resolve())


def flatten_dict(prefix: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in sorted(payload.items()):
        flat_key = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            out.update(flatten_dict(flat_key, value))
        else:
            out[flat_key] = value
    return out
