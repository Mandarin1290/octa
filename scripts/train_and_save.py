#!/usr/bin/env python3
"""Demo trainer (legacy) + optional production pipeline runner.

Legacy mode (default): trains a simple sklearn model and writes into `artifacts/`.

Pipeline mode (recommended for real data):
    ./.venv/bin/python scripts/train_and_save.py \
        --symbol AAPL \
        --config configs/e2e_real_raw.yaml

To also write a non-tradeable debug artifact on gate FAIL:
    ./.venv/bin/python scripts/train_and_save.py \
        --symbol AAPL \
        --config configs/e2e_real_raw_debug.yaml \
        --debug-on-fail
"""

from __future__ import annotations

import argparse
import os
import pickle
import random
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import KFold, TimeSeriesSplit, cross_validate

# sklearn / lgbm trainer for Tier-1 pipeline
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    import lightgbm as lgb
    LGB_AVAILABLE = True
except Exception:
    LGB_AVAILABLE = False

try:
    import optuna
    OPTUNA_AVAILABLE = True
except Exception:
    optuna = None
    OPTUNA_AVAILABLE = False
from sklearn.linear_model import Ridge

try:
    from octa_atlas.models import ArtifactMetadata
    from octa_atlas.registry import AtlasRegistry
    from octa_fabric.fingerprint import sha256_hexdigest
except Exception:
    # lightweight fallback for local dev when octa_atlas is not installed
    import json
    from dataclasses import dataclass
    from pathlib import Path

    @dataclass
    class ArtifactMetadata:
        asset_id: str
        artifact_type: str
        version: str
        created_at: str
        dataset_hash: str
        training_window: str
        feature_spec_hash: str
        hyperparams: dict
        metrics: dict
        code_fingerprint: str
        gate_status: str


    class AtlasRegistry:
        def __init__(self, root: str = "artifacts"):
            self.root = Path(root)

        def save_artifact(self, asset_id: str, artifact_type: str, version: str, state: dict, metadata: ArtifactMetadata):
            out = self.root / "models" / asset_id / artifact_type / version
            out.mkdir(parents=True, exist_ok=True)
            # save state and metadata
            (out / "state.json").write_text(json.dumps(state))
            (out / "metadata.json").write_text(json.dumps(metadata.__dict__))

    def sha256_hexdigest(obj: object) -> str:
        import hashlib
        import json

        h = hashlib.sha256()
        h.update(json.dumps(obj, sort_keys=True).encode())
        return h.hexdigest()
import sys

# Allow running this file directly: `python scripts/train_and_save.py ...`
# (When invoked as a script, Python doesn't include repo root on sys.path.)
_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from scripts.mlflow_helper import (
    available,
    log_artifacts,
    log_metrics,
    log_params,
    log_pyfunc_model_and_register,
    start_run,
)


def make_estimator(params: dict | None = None):
    """Return a sklearn Pipeline estimator: StandardScaler + LGBMRegressor (fallback to Ridge)."""
    if params is None:
        params = {}
    if LGB_AVAILABLE:
        core = lgb.LGBMRegressor(**{k: v for k, v in params.items() if k in ("num_leaves", "n_estimators", "learning_rate", "max_depth", "min_child_samples")})
    else:
        core = Ridge(alpha=float(params.get("alpha", 1.0)))
    pipe = Pipeline([("scaler", StandardScaler()), ("model", core)])
    return pipe


def make_synthetic(n=200):
    # simple linear relation y = 3*x + small deterministic noise
    X = [i / 10.0 for i in range(n)]
    y = [3.0 * x + ((i % 5) - 2) * 0.01 for i, x in enumerate(X)]
    return X, y


def main():
    # use artifacts/ as atlas root
    atlas = AtlasRegistry(root="artifacts")

    p = argparse.ArgumentParser()
    # Optional: run the production training pipeline instead of the legacy demo trainer.
    p.add_argument("--symbol", default=None, help="Run octa_training pipeline for this symbol (optional)")
    p.add_argument("--config", default=None, help="Path to a training config YAML for pipeline runs (optional)")
    p.add_argument("--run-id", default=None, help="Run id for pipeline runs (optional)")
    p.add_argument(
        "--debug-on-fail",
        action="store_true",
        help="For pipeline runs: write debug .pkl even on gate FAIL (non-tradeable, isolated debug_dir)",
    )
    p.add_argument(
        "--safe-mode",
        action="store_true",
        help="For pipeline runs: never write tradeable artifacts (PASS packaging disabled)",
    )
    p.add_argument("--parquet", default=None, help="Path to input parquet file (optional)")
    p.add_argument("--target", default=None, help="Target column name (optional)")
    p.add_argument("--version", default="v1", help="Model version")
    p.add_argument("--seed", type=int, default=42, help="Random seed for deterministic runs")
    p.add_argument("--cv-folds", type=int, default=0, help="Run k-fold CV if >0")
    p.add_argument("--backtest", action="store_true", help="Run walk-forward backtest for timeseries data")
    p.add_argument("--hyperopt", action="store_true", help="Run simple hyperparameter search before final training")
    p.add_argument(
        "--fast",
        action="store_true",
        help="For pipeline runs: reduce model set and stop after first successful model (faster diagnostics)",
    )
    p.add_argument("--gate-manifest", default=None, help="Path to gate manifest or directory for safety lock check")
    args = p.parse_args()

    # Pipeline mode: if a symbol is provided, run octa_training end-to-end and exit.
    if args.symbol:
        from core.training_safety_lock import (
            TrainingSafetyLockError,
            assert_training_armed,
        )
        from octa_training.core.config import load_config
        from octa_training.core.pipeline import train_evaluate_package
        from octa_training.core.state import StateRegistry

        cfg_path = args.config
        # Convenience: when caller didn't specify a config but wants debug-on-fail,
        # prefer the isolated e2e config if present.
        if args.debug_on_fail and (cfg_path is None or cfg_path == "configs/e2e_real_raw.yaml"):
            cfg_path = "configs/e2e_real_raw_debug.yaml"

        cfg = load_config(cfg_path) if cfg_path else load_config()
        # If user asked for debug-on-fail with a custom config, enforce at runtime.
        if args.debug_on_fail:
            try:
                cfg.packaging.save_debug_on_fail = True
            except Exception:
                pass

        state = StateRegistry(cfg.paths.state_dir)
        run_id = args.run_id or f"demo_pipeline_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        if not args.safe_mode:
            try:
                assert_training_armed(cfg, args.symbol, "1D", manifest_path_or_dir=args.gate_manifest)
            except TrainingSafetyLockError as e:
                print(f"Training blocked by safety lock: {e}")
                return
        res = train_evaluate_package(
            args.symbol,
            cfg,
            state,
            run_id=run_id,
            safe_mode=bool(args.safe_mode),
            smoke_test=False,
            parquet_path=str(args.parquet) if args.parquet else None,
            fast=bool(args.fast),
        )

        pack = getattr(res, "pack_result", None)
        passed = bool(getattr(res, 'passed', False))
        print(f"Pipeline result symbol={args.symbol} passed={passed}")
        if not passed:
            gate_obj = getattr(res, "gate_result", None)
            reasons = getattr(gate_obj, "reasons", None) if gate_obj is not None else None
            if isinstance(reasons, list) and reasons:
                print("gate_reasons=", ";".join([str(r) for r in reasons[:5]]))
            err = getattr(res, "error", None)
            if err:
                print(f"error={err}")
        if pack:
            print(f"pack_result={pack}")
        else:
            print("pack_result=None")
        return

    if args.parquet:
        from scripts.asset_detector import detect_from_path
        profile = detect_from_path(args.parquet)
        # choose default target if not provided: last numeric column
        num_cols = profile.get('numeric_cols', [])
        if not num_cols:
            raise RuntimeError("No numeric columns found in parquet to train on")
        target = args.target or num_cols[-1]
        if target not in profile.get('columns', []):
            raise RuntimeError(f"Target column {target} not found in input")
        from scripts.preprocessing import preprocess_df, save_spec

        df = pd.read_parquet(args.parquet)
        X_df, y_ser, spec = preprocess_df(df, target=target, spec_name=f"spec_{args.version}")
        # convert X_df rows to list-of-lists for SimpleLinearModel
        X = X_df.values.tolist()
        y = y_ser.astype(float).tolist() if y_ser is not None else []
        save_spec(spec, name=f"spec_{args.version}")
        feature_cols = X_df.columns.tolist()
        dataset_hash = sha256_hexdigest({"rows": len(df), "cols": feature_cols, "asset_type": profile.get('asset_type')})

        # optional hyperparameter search (choose number of features)
        if args.hyperopt:
            try:
                from scripts.hyperparam_search import optuna_feature_search

                def _factory():
                    return make_estimator(None)

                max_f = min(4, len(feature_cols))
                res = optuna_feature_search(_factory, X, y, max_features=max_f, n_trials=10, seed=args.seed)
                best = res.get('best') if isinstance(res, dict) else None
                if best:
                    keep_idx = best.get('keep_idx', [])
                    feature_cols = [feature_cols[i] for i in keep_idx]
                    # reduce X accordingly
                    X = [[r[i] for i in keep_idx] for r in X]
                    # persist updated spec
                    spec['features'] = [f for f in spec['features'] if f['name'] in feature_cols]
                    save_spec(spec, name=f"spec_{args.version}")
            except Exception:
                pass
    else:
        X, y = make_synthetic(200)
        dataset_hash = sha256_hexdigest({"n": len(X), "sum_x": sum(X), "sum_y": sum(y)})

    # build estimator
    combined = list(zip(X, y, strict=False))
    random.Random(args.seed).shuffle(combined)
    split = int(0.8 * len(combined))
    train = combined[:split]
    test = combined[split:]
    X_train = [r[0] for r in train]
    y_train = [r[1] for r in train]
    X_test = [r[0] for r in test]
    y_test = [r[1] for r in test]

    # Optuna hyperparameter search (optional)
    chosen_params = {}
    if args.hyperopt:
        def objective(trial):
            if LGB_AVAILABLE:
                params = {
                    "num_leaves": trial.suggest_int("num_leaves", 16, 128),
                    "n_estimators": trial.suggest_int("n_estimators", 50, 500),
                    "learning_rate": trial.suggest_loguniform("learning_rate", 1e-3, 0.3),
                    "min_child_samples": trial.suggest_int("min_child_samples", 5, 50),
                }
            else:
                params = {"alpha": trial.suggest_loguniform("alpha", 1e-3, 10.0)}

            est = make_estimator(params)
            # simple CV on training fold
            try:
                cv = KFold(n_splits=3, shuffle=True, random_state=args.seed)
                X_arr = np.array(X_train)
                if X_arr.ndim == 1:
                    X_arr = X_arr.reshape(-1, 1)
                y_arr = np.array(y_train)
                scores = cross_validate(est, X_arr, y_arr, scoring=("neg_mean_squared_error",), cv=cv, n_jobs=1)
                return float(-np.mean(scores["test_neg_mean_squared_error"]))
            except Exception:
                return float('inf')

        if OPTUNA_AVAILABLE:
            study = optuna.create_study(direction="minimize", sampler=optuna.samplers.TPESampler(seed=args.seed))
            print("Warning: legacy train_and_save.py uses hardcoded n_trials=20 for Optuna (adjust in script if needed).")
            study.optimize(objective, n_trials=20)
            chosen_params = study.best_params or {}
        else:
            print("Optuna not available; skipping hyperopt")

    estimator = make_estimator(chosen_params if chosen_params else None)
    # train final model on train set
    try:
        X_train_arr = np.array(X_train)
        if X_train_arr.ndim == 1:
            X_train_arr = X_train_arr.reshape(-1, 1)
        y_train_arr = np.array(y_train)
        estimator.fit(X_train_arr, y_train_arr)
    except Exception:
        # fallback
        estimator.fit(np.array(X_train).reshape(-1, 1) if np.array(X_train).ndim == 1 else np.array(X_train), np.array(y_train))

    X_test_arr = np.array(X_test)
    if X_test_arr.ndim == 1:
        X_test_arr = X_test_arr.reshape(-1, 1)
    preds = estimator.predict(X_test_arr)
    preds = [float(p) for p in preds]
    y_true = [float(v) for v in y_test]
    mse = float(mean_squared_error(y_true, preds)) if preds else 0.0
    mae = float(mean_absolute_error(y_true, preds)) if preds else 0.0

    metadata = ArtifactMetadata(
        asset_id="demo_model",
        artifact_type="regression",
        version=args.version,
        created_at=datetime.now(timezone.utc).isoformat(),
        dataset_hash=dataset_hash,
        training_window=("synthetic" if not args.parquet else f"file:{args.parquet}"),
        feature_spec_hash=sha256_hexdigest({"features": feature_cols if args.parquet else []}),
        hyperparams=chosen_params if chosen_params else {},
        metrics={"mse": mse, "mae": mae, "n_train": float(len(X_train)), "n_test": float(len(X_test))},
        code_fingerprint=sha256_hexdigest({"module": "train_and_save"}),
        gate_status="COMPLETE",
    )

    # save sklearn pipeline
    model_state = estimator
    atlas.save_artifact("demo_model", "regression", args.version, {}, metadata)
    out = Path("artifacts") / "models" / "demo_model" / "regression" / args.version
    out.mkdir(parents=True, exist_ok=True)
    pkl_path = out / "model.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump(model_state, f)
    print("Saved demo model to", out)

    # Optional: cross-validation (on training set)
    cv_results = None
    try:
        if args.cv_folds and args.cv_folds > 1:
            X_arr = np.array(X_train)
            y_arr = np.array(y_train)
            if args.backtest:
                tss = TimeSeriesSplit(n_splits=args.cv_folds)
                cv = tss
            else:
                cv = KFold(n_splits=args.cv_folds, shuffle=True, random_state=args.seed)
            scoring = ("neg_mean_squared_error",)
            res = cross_validate(estimator, X_arr, y_arr, scoring=scoring, cv=cv, return_train_score=False)
            mse_scores = [-v for v in res.get("test_neg_mean_squared_error", [])]
            cv_results = {"mse_scores": mse_scores, "mse_mean": float(np.mean(mse_scores)) if mse_scores else None, "mse_std": float(np.std(mse_scores, ddof=1)) if len(mse_scores) > 1 else 0.0}
            metadata.metrics.update({"cv": cv_results})
    except Exception:
        cv_results = None

    # Optional: backtest if timeseries and requested
    backtest_results = None
    try:
        if args.backtest:
            from scripts.backtest import walk_forward_backtest

            # provide factory that creates a fresh estimator with chosen params
            def factory():
                return make_estimator(chosen_params if chosen_params else None)

            backtest_results = walk_forward_backtest(factory, X_train + X_test, y_train + y_test, initial_window=max(3, int(0.2 * len(X_train))))
            metadata.metrics.update({"backtest": backtest_results})
    except Exception:
        backtest_results = None

    # Write model card
    try:
        from scripts.model_card import write_model_card

        write_model_card(str(out), metadata, cv_results=cv_results, backtest_results=backtest_results)
    except Exception:
        pass

    # MLflow logging (optional) + register model

    with start_run("demo_model") as run:
        log_params({"n": len(X), "dataset_hash": metadata.dataset_hash})
        # no mae for synthetic run, but log n
        log_metrics({"n": float(len(X))})
        log_artifacts(out)
        # also log feature spec if available
        try:
            spec_file = Path("artifacts/feature_specs") / f"spec_{args.version}.json"
            if spec_file.exists():
                log_artifacts(str(spec_file))
        except Exception:
            pass
        if available():
            # attempt to register the artifact directory under the model registry
            try:
                run_id = getattr(run, "info", None) and getattr(run.info, "run_id", None) or None
                # prefer our pyfunc wrapper if available
                try:
                    from scripts.pyfunc_wrapper import log_pyfunc_model

                    log_pyfunc_model(model_state, artifact_path="model", registered_name="demo_model", run_id=run_id)
                except Exception:
                    # fallback to existing helper
                    try:
                        log_pyfunc_model_and_register(
                            model_state,
                            artifact_path="model",
                            registered_name="demo_model",
                            run_id=run_id,
                            promote_on_valid=True,
                            validation_passed=True,
                        )
                    except Exception:
                        pass
            except Exception:
                pass
        else:
            print("MLflow not available; skipped MLflow logging")

    # write integrity and optional GPG signature for model.pkl
    try:
        from scripts.sign_artifact import gpg_sign, write_integrity

        integrity = write_integrity(str(pkl_path))
        print("Wrote integrity file:", integrity)
        gsig = gpg_sign(str(pkl_path), key=os.getenv("GPG_KEY"))
        if gsig:
            print("Wrote GPG signature:", gsig)
    except Exception:
        pass


if __name__ == "__main__":
    main()
