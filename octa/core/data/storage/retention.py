from __future__ import annotations

import gzip
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List


def _now() -> datetime:
    return datetime.utcnow()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass(frozen=True)
class RetentionPolicy:
    enabled: bool = True
    older_than_days: int = 30
    copy_compressed_only: bool = True  # do NOT remove originals by default


def compress_old_files(*, roots: List[str], policy: RetentionPolicy) -> Dict[str, List[str]]:
    """Copy-compress old files under roots.

    Fail-safe defaults:
    - Does not delete originals (copy_compressed_only=True)
    - Only targets .log/.jsonl/.ndjson by default

    Returns a summary dict with created artifacts.
    """

    if not policy.enabled:
        return {"created": []}

    created: List[str] = []
    cutoff = _now() - timedelta(days=int(policy.older_than_days))

    for root in roots:
        rp = Path(root)
        if not rp.exists():
            continue
        for p in rp.rglob("*"):
            try:
                if not p.is_file():
                    continue
                if p.suffix.lower() not in {".log", ".jsonl", ".ndjson"}:
                    continue
                mt = datetime.utcfromtimestamp(p.stat().st_mtime)
                if mt > cutoff:
                    continue

                gz = p.with_suffix(p.suffix + ".gz")
                if gz.exists():
                    continue

                with p.open("rb") as src, gzip.open(str(gz), "wb", compresslevel=6) as dst:
                    dst.write(src.read())
                created.append(str(gz))
            except Exception:
                continue

    return {"created": created}


__all__ = ["RetentionPolicy", "compress_old_files", "_sha256_file"]
