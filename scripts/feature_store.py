"""Lightweight local feature store (Feast-like offline features).

Functions:
- materialize_features_for_asset(src_parquet, asset, out_root, version=None)
- get_latest_features_path(asset, out_root)

Stores features as Parquet under `artifacts/features/<asset>/<version>/features.parquet`
and writes `meta.json` alongside.
"""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _sha256_of_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def materialize_features_for_asset(src_parquet: str | Path, asset: str, out_root: Path | str = "artifacts/features", version: str | None = None) -> dict:
    src = Path(src_parquet)
    if not src.exists():
        raise FileNotFoundError(src)
    df = pd.read_parquet(src)
    # prefer 'close' column for finance returns
    if "close" in df.columns:
        s = df["close"].astype(float)
        ret = s.pct_change().dropna()
        if len(ret) < 10:
            raise ValueError("not enough data for features")
        lag1 = ret.shift(1).dropna()
        roll5 = ret.rolling(5).mean().shift(1).dropna()
        feat_df = pd.concat([lag1, roll5, ret], axis=1).dropna()
        feat_df.columns = ["lag1", "roll5", "target"]
    else:
        nums = df.select_dtypes(include="number")
        if nums.shape[1] == 0:
            raise ValueError("no numeric columns to build features")
        if nums.shape[1] == 1:
            s = nums.iloc[:, 0].astype(float)
            feat_df = pd.concat([s.shift(1).dropna(), s.iloc[1:].astype(float)], axis=1)
            feat_df.columns = ["lag1", "target"]
        else:
            nums = nums.astype(float)
            # simple multivariate: use all but last as features, last as target
            feat_df = nums.dropna()

    out_root = Path(out_root)
    if version is None:
        version = datetime.now(timezone.utc).strftime("v%Y%m%d%H%M%S")
    dest = out_root / asset / version
    dest.mkdir(parents=True, exist_ok=True)
    out_path = dest / "features.parquet"
    feat_df.to_parquet(out_path)

    meta = {
        "asset": asset,
        "version": version,
        "source": str(src),
        "stored_path": str(out_path),
        "rows": int(len(feat_df)),
        "cols": list(feat_df.columns),
        "sha256": _sha256_of_file(out_path),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (dest / "meta.json").write_text(json.dumps(meta, indent=2))
    return meta


def get_latest_features_path(asset: str, out_root: Path | str = "artifacts/features") -> Path | None:
    root = Path(out_root) / asset
    if not root.exists():
        return None
    versions = [d for d in root.iterdir() if d.is_dir()]
    if not versions:
        return None
    # pick newest by directory name (vTIMESTAMP) or by meta created_at
    versions_sorted = sorted(versions, reverse=True)
    for v in versions_sorted:
        candidate = v / "features.parquet"
        if candidate.exists():
            return candidate
    return None
