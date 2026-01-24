#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd

from octa_atlas.models import ArtifactMetadata
from octa_atlas.registry import AtlasRegistry
from octa_fabric.fingerprint import sha256_hexdigest


class SimpleLinearModel:
    def __init__(self):
        self.coef_ = []
        self.intercept_ = 0.0

    def fit(self, X: List[List[float]] | List[float], y: List[float]):
        if not X:
            raise ValueError("empty dataset")
        if isinstance(X[0], (int, float)):
            xs = [[float(x)] for x in X]
        else:
            xs = [[float(v) for v in row] for row in X]
        n = len(xs)
        m = len(xs[0])
        XT_X = [[0.0] * m for _ in range(m)]
        XT_y = [0.0] * m
        for i in range(n):
            for a in range(m):
                for b in range(m):
                    XT_X[a][b] += xs[i][a] * xs[i][b]
                XT_y[a] += xs[i][a] * float(y[i])
        aug = [row[:] + [XT_y[i]] for i, row in enumerate(XT_X)]
        for i in range(m):
            piv = i
            for r in range(i, m):
                if abs(aug[r][i]) > abs(aug[piv][i]):
                    piv = r
            aug[i], aug[piv] = aug[piv], aug[i]
            pivval = aug[i][i]
            if abs(pivval) < 1e-12:
                self.coef_ = [0.0] * m
                self.intercept_ = sum(y) / n
                return
            aug[i] = [v / pivval for v in aug[i]]
            for r in range(m):
                if r == i:
                    continue
                factor = aug[r][i]
                if factor:
                    aug[r] = [aug[r][c] - factor * aug[i][c] for c in range(m + 1)]
        beta = [aug[i][-1] for i in range(m)]
        self.coef_ = beta
        self.intercept_ = 0.0


def choose_parquet_file(root: Path) -> Optional[Path]:
    files = list(root.rglob("*.parquet"))
    if not files:
        return None
    # prefer daily/small files
    for name in ("daily", "_daily", "10y", "intraday", "_from_intraday"):
        for f in files:
            if name in f.name.lower():
                return f
    # otherwise pick smallest file to be safe
    files_sorted = sorted(files, key=lambda p: p.stat().st_size)
    return files_sorted[0]


def load_sample(path: Path, max_rows: int = 2000) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if len(df) > max_rows:
        return df.head(max_rows)
    return df


def prepare_xy(df: pd.DataFrame) -> tuple[list[Any], list[float], str]:
    nums = df.select_dtypes(include="number")
    if nums.shape[1] == 0:
        raise SystemExit("No numeric columns found to train on")
    target = nums.columns[-1]
    if nums.shape[1] == 1:
        s = nums[target].astype(float)
        X = s.shift(1).dropna().tolist()
        y = s.iloc[1:].astype(float).tolist()
    else:
        X_df = nums.drop(columns=[target])
        X = X_df.astype(float).values.tolist()
        y = nums[target].astype(float).tolist()
    return X, y, str(target)


def main():
    raw = Path("raw")
    chosen = choose_parquet_file(raw)
    if chosen is None:
        raise SystemExit("No parquet files found under raw/")
    print("Selected sample file:", chosen)
    df = load_sample(chosen, max_rows=2000)
    print("Loaded rows,cols:", df.shape)

    X, y, target = prepare_xy(df)

    model = SimpleLinearModel()
    model.fit(X, y)

    atlas = AtlasRegistry(root="artifacts")
    dataset_hash = sha256_hexdigest({"rows": len(df), "cols": list(df.columns)[:10]})
    version = datetime.now(timezone.utc).strftime("v%Y%m%d%H%M%S")
    metadata = ArtifactMetadata(
        asset_id=chosen.stem,
        artifact_type="regression",
        version=version,
        created_at=datetime.now(timezone.utc).isoformat(),
        dataset_hash=dataset_hash,
        training_window="raw_sample",
        feature_spec_hash="",
        hyperparams={},
        metrics={"n": float(len(y)), "target": target},
        code_fingerprint=sha256_hexdigest({"module": "train_on_raw"}),
        gate_status="COMPLETE",
    )

    model_state = {"coef": model.coef_, "intercept": model.intercept_, "target": target}
    atlas.save_artifact(chosen.stem, "regression", version, model_state, metadata)
    print(f"Saved model to artifacts/models/{chosen.stem}/regression/{version}")


if __name__ == "__main__":
    main()
