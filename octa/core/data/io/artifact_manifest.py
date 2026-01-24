from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from octa.core.data.storage.retention import _sha256_file


@dataclass(frozen=True)
class ManifestEntry:
    path: str
    size_bytes: int
    sha256: str


def build_manifest(*, root: str, include_globs: Optional[List[str]] = None) -> Dict[str, Any]:
    """Create a content-addressed manifest to avoid duplicates.

    No deletions are performed. This is purely a registry of existing files.
    """

    rp = Path(root)
    include_globs = include_globs or ["**/*.pkl", "**/*.json", "**/*.jsonl", "**/*.parquet"]
    files: List[Path] = []
    for g in include_globs:
        files.extend(sorted(rp.glob(g)))

    entries: List[Dict[str, Any]] = []
    for p in files:
        try:
            if not p.is_file():
                continue
            st = p.stat()
            entries.append({
                "path": str(p.as_posix()),
                "size_bytes": int(st.st_size),
                "sha256": _sha256_file(p),
            })
        except Exception:
            continue

    return {"root": str(rp.as_posix()), "entries": entries}


def write_manifest(*, root: str, out_path: str) -> str:
    obj = build_manifest(root=root)
    op = Path(out_path)
    op.parent.mkdir(parents=True, exist_ok=True)
    op.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(op)


__all__ = ["build_manifest", "write_manifest", "ManifestEntry"]
