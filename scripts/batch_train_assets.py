#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pandas as pd

from octa_atlas.models import ArtifactMetadata
from octa_atlas.registry import AtlasRegistry
from octa_fabric.fingerprint import sha256_hexdigest
from scripts.feature_store import materialize_features_for_asset
from scripts.mlflow_helper import (
    available,
    log_artifacts,
    log_metrics,
    log_params,
    start_run,
)


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

    def predict(self, X: List[List[float]] | List[float]):
        if isinstance(X, (int, float)):
            xs = [float(X)]
        else:
            xs = X
        if isinstance(xs[0], (int, float)):
            return [self.coef_[0] * float(x) + self.intercept_ for x in xs]
        else:
            res = []
            for row in xs:
                s = 0.0
                for c, v in zip(self.coef_, row, strict=False):
                    s += c * float(v)
                s += self.intercept_
                res.append(s)
            return res


def features_from_close(s: pd.Series) -> tuple[List[List[float]], List[float]]:
    # compute lag1 return and rolling mean(5) of return; predict next return
    ret = s.pct_change().dropna()
    if len(ret) < 10:
        raise ValueError("not enough data")
    lag1 = ret.shift(1).dropna()
    roll5 = ret.rolling(5).mean().shift(1).dropna()
    # align
    df = pd.concat([lag1, roll5, ret], axis=1).dropna()
    df.columns = ["lag1", "roll5", "target"]
    X = df[["lag1", "roll5"]].values.tolist()
    y = df["target"].astype(float).tolist()
    return X, y


def train_on_file(path: Path, atlas: AtlasRegistry, max_rows: int = 2000) -> dict:
    # create a reproducible local dataset version and record metadata
    try:
        subprocess.check_call(["python3", "scripts/record_dataset_version.py", str(path)])
    except Exception:
        pass

    # materialize offline features from the dataset snapshot into feature store
    try:
        # find latest dataset snapshot for this asset
        ds_root = Path("artifacts/datasets") / path.stem
        version_dirs = sorted([d for d in ds_root.iterdir() if d.is_dir()], reverse=True) if ds_root.exists() else []
        if version_dirs:
            ds_parquet = version_dirs[0] / "data.parquet"
            try:
                feat_meta = materialize_features_for_asset(ds_parquet, path.stem)
                feat_path = feat_meta.get("stored_path")
            except Exception:
                feat_path = None
        else:
            feat_path = None
    except Exception:
        feat_path = None

    if feat_path:
        df = pd.read_parquet(feat_path)
        # if feature df has columns ['lag1','roll5','target'] or ['lag1','target'] handle below
        if "target" in df.columns:
            X = df.drop(columns=["target"]).astype(float).values.tolist()
            y = df["target"].astype(float).tolist()
        else:
            # fallback to raw
            df = pd.read_parquet(path)
    else:
        df = pd.read_parquet(path)
    if len(df) > max_rows:
        df = df.head(max_rows)
    # prefer 'close' column
    if "close" in df.columns:
        s = df["close"].astype(float)
        X, y = features_from_close(s)
        target_name = "return"
    else:
        nums = df.select_dtypes(include="number")
        if nums.shape[1] == 0:
            raise ValueError("no numeric columns")
        if nums.shape[1] == 1:
            s = nums.iloc[:, 0].astype(float)
            X = s.shift(1).dropna().tolist()
            y = s.iloc[1:].astype(float).tolist()
        else:
            X = nums.iloc[:, :-1].astype(float).values.tolist()
            y = nums.iloc[:, -1].astype(float).tolist()
        target_name = nums.columns[-1]

    model = SimpleLinearModel()
    model.fit(X, y)

    preds = model.predict(X)
    # simple MAE
    mae = float(sum(abs(p - t) for p, t in zip(preds, y, strict=False)) / len(y)) if y else float("nan")

    dataset_hash = sha256_hexdigest({"rows": len(df), "cols": list(df.columns)[:10]})
    version = datetime.now(timezone.utc).strftime("v%Y%m%d%H%M%S")
    metadata = ArtifactMetadata(
        asset_id=path.stem,
        artifact_type="regression",
        version=version,
        created_at=datetime.now(timezone.utc).isoformat(),
        dataset_hash=dataset_hash,
        training_window="raw_batch",
        feature_spec_hash="",
        hyperparams={},
        metrics={"n": float(len(y)), "mae": mae, "target": target_name},
        code_fingerprint=sha256_hexdigest({"module": "batch_train_assets"}),
        gate_status="COMPLETE",
    )

    model_state = {"coef": model.coef_, "intercept": model.intercept_, "target": target_name}
    atlas.save_artifact(path.stem, "regression", version, model_state, metadata)
    # MLflow logging per-asset (optional)
    out = Path("artifacts") / "models" / path.stem / "regression" / version
    from scripts.mlflow_helper import register_model

    with start_run(path.stem) as run:
        log_params({"asset": path.stem, "version": version})
        log_metrics({"mae": mae, "n": float(len(y))})
        log_artifacts(out)
        if available():
            try:
                run_id = getattr(run, "info", None) and getattr(run.info, "run_id", None) or None
                rel = f"models/{path.stem}/regression/{version}"
                register_model(rel, path.stem, run_id=run_id)
            except Exception:
                pass
        else:
            print("MLflow not available; skipped MLflow logging for", path.stem)

    return {"asset": path.stem, "version": version, "mae": mae, "n": len(y)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-assets", type=int, default=5)
    parser.add_argument("--max-rows", type=int, default=2000)
    args = parser.parse_args()

    raw = Path("raw")
    files = sorted(raw.glob("*.parquet"))
    selected = files[: args.max_assets]
    atlas = AtlasRegistry(root="artifacts")
    results = []
    for p in selected:
        try:
            print("Training on", p)
            r = train_on_file(p, atlas, max_rows=args.max_rows)
            results.append(r)
            print("Saved", r)
            # run drift check between previous snapshot and current snapshot (if exists)
            try:
                # locate dataset versions
                ds_root = Path("artifacts/datasets") / p.stem
                versions = sorted([d for d in ds_root.iterdir() if d.is_dir()]) if ds_root.exists() else []
                if len(versions) >= 2:
                    prev = versions[-2] / "data.parquet"
                    curr = versions[-1] / "data.parquet"
                    outdir = Path("artifacts") / "drift_reports" / p.stem / r["version"]
                    import json
                    import subprocess

                    cmd = ["python3", "scripts/evidently_drift.py", "--reference", str(prev), "--current", str(curr), "--out-dir", str(outdir)]
                    proc = subprocess.run(cmd, capture_output=True, text=True)
                    if proc.returncode == 0:
                        try:
                            drift_res = json.loads(proc.stdout)
                        except Exception:
                            drift_res = {"raw": proc.stdout}
                        # attach drift summary into results
                        results[-1]["drift"] = drift_res.get("drift_summary", {})
                        print("Drift result:", drift_res.get("drift_summary", {}))
                    else:
                        print("Drift script failed:", proc.stderr)
            except Exception as e:
                print("Drift check skipped/error:", e)
        except Exception as e:
            print("Skipping", p, "error:", e)

    # write summary
    out = Path("artifacts") / "batch_training_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2))
    print("Summary written to", out)


if __name__ == "__main__":
    main()
