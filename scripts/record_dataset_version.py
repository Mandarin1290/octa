#!/usr/bin/env python3
"""Create a reproducible local dataset version for a Parquet file.

Copies the source parquet into `artifacts/datasets/<asset>/<version>/data.parquet`
and writes `meta.json` containing source path, sha256, row/col counts and timestamp.
"""
import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def sha256_of_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def record(src: Path, out_root: Path, version: str | None = None) -> dict:
    if not src.exists():
        raise FileNotFoundError(src)
    asset = src.stem
    if version is None:
        version = datetime.now(timezone.utc).strftime("v%Y%m%d%H%M%S")

    dest = out_root / asset / version
    dest.mkdir(parents=True, exist_ok=True)
    data_dest = dest / "data.parquet"
    shutil.copy2(src, data_dest)

    sha = sha256_of_file(data_dest)

    meta = {
        "asset": asset,
        "version": version,
        "source": str(src),
        "stored_path": str(data_dest),
        "sha256": sha,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (dest / "meta.json").write_text(json.dumps(meta, indent=2))
    return meta


def main():
    p = argparse.ArgumentParser()
    p.add_argument("src", type=str)
    p.add_argument("--out-root", type=str, default="artifacts/datasets")
    p.add_argument("--version", type=str, default=None)
    args = p.parse_args()
    meta = record(Path(args.src), Path(args.out_root), args.version)
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
