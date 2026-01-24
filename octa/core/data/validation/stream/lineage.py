from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict

import pyarrow.parquet as pq


def parquet_content_hash(path: str) -> str:
    p = Path(path)
    # include file bytes and parquet metadata
    h = hashlib.sha256()
    with p.open("rb") as fh:
        while True:
            chunk = fh.read(8192)
            if not chunk:
                break
            h.update(chunk)
    # include parquet metadata for determinism
    try:
        meta = pq.ParquetFile(str(path)).metadata
        h.update(str(meta.num_rows).encode())
        h.update(str(meta.num_row_groups).encode())
    except Exception:
        pass
    return h.hexdigest()


def minimal_parquet_metadata(path: str) -> Dict[str, int]:
    pf = pq.ParquetFile(str(path))
    return {
        "num_rows": pf.metadata.num_rows,
        "num_row_groups": pf.metadata.num_row_groups,
    }
