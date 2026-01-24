from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.ensemble import (
    RandomForestClassifier,
    RandomForestRegressor,
)
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.preprocessing import StandardScaler

from octa_training.core.device import (
    safe_set_cat_params,
    safe_set_lgbm_params,
    safe_set_xgb_params,
)
from octa_training.core.optuna_tuner import tune_model
from octa_training.core.splits import SplitFold

# optional progress bar for long trainings
try:
    import os
    import sys

    from tqdm.auto import tqdm as _tqdm_impl

    class _BrokenPipeSafeFile:
        def __init__(self, f):
            self._f = f

        def write(self, s):
            try:
                return self._f.write(s)
            except BrokenPipeError:
                return 0

        def flush(self):
            try:
                return self._f.flush()
            except BrokenPipeError:
                return None

        def isatty(self):
            try:
                return bool(self._f.isatty())
            except Exception:
                return False

        def __getattr__(self, name):
            return getattr(self._f, name)

    def _tqdm(x, **kw):
        # Allow callers (e.g. diagnose sweeps) to silence tqdm without code changes.
        if os.environ.get("OCTA_DISABLE_TQDM", "").strip().lower() in {"1", "true", "yes", "on"}:
            kw.setdefault("disable", True)
        # Default: avoid progress bar writes to pipes/non-ttys (prevents BrokenPipe failures under piping/tee).
        kw.setdefault("disable", not bool(getattr(sys.stderr, "isatty", lambda: False)()))
        # Also ensure tqdm writes are BrokenPipe-safe.
        kw.setdefault("file", _BrokenPipeSafeFile(sys.stderr))
        return _tqdm_impl(x, **kw)
except Exception:  # pragma: no cover - optional dependency
    def _tqdm(x, **kw):
        return x


@dataclass
class FoldMetric:
    fold: int
    metric: Dict[str, float]


@dataclass
class TrainResult:
    model_name: str
    task: str
    horizon: str
    fold_metrics: List[FoldMetric]
    oof_predictions: Dict[str, Any]
    feature_importance: Optional[Dict[str, float]]
    params: Dict[str, Any]
    device_used: str


def _metrics_cls(y_true, y_prob, y_pred):
    res = {}
    try:
        res["auc"] = float(roc_auc_score(y_true, y_prob))
    except Exception:
        res["auc"] = float("nan")
    try:
        res["logloss"] = float(log_loss(y_true, y_prob))
    except Exception:
        res["logloss"] = float("nan")
    try:
        res["brier"] = float(brier_score_loss(y_true, y_prob))
    except Exception:
        res["brier"] = float("nan")
    return res


def _metrics_reg(y_true, y_pred):
    res = {}
    try:
        res["rmse"] = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    except Exception:
        res["rmse"] = float("nan")
    try:
        res["mae"] = float(mean_absolute_error(y_true, y_pred))
    except Exception:
        res["mae"] = float("nan")
    try:
        res["dir_acc"] = float((np.sign(y_true) == np.sign(y_pred)).mean())
    except Exception:
        res["dir_acc"] = float("nan")
    return res


def train_models(
    X: pd.DataFrame,
    y_dict: Dict[str, pd.Series],
    splits: List[Any],
    settings: Any,
    device_profile,
    fast: bool = False,
    prices: Optional[pd.Series] = None,
    eval_settings: Optional[Any] = None,
) -> List[TrainResult]:
    """Train models per horizon and produce OOF predictions.

    Contract:
    - Returns a list of TrainResult.
    - Each TrainResult.oof_predictions contains {"index": [...], "pred": [...]}.
    - OOF preds are aligned to the original X index; NaNs are allowed.
    """

    from sklearn.ensemble import (
        ExtraTreesClassifier,
        ExtraTreesRegressor,
        HistGradientBoostingClassifier,
        HistGradientBoostingRegressor,
    )

    seed = int(getattr(settings, "seed", 42))
    scale_linear = bool(getattr(settings, "scale_linear", True))

    compute_strategy_metrics = prices is not None and eval_settings is not None
    if compute_strategy_metrics:
        try:
            from octa_training.core.evaluation import compute_equity_and_metrics
        except Exception:
            compute_strategy_metrics = False

    tuning_enabled = False
    try:
        tuning_enabled = bool(getattr(getattr(settings, "tuning", None), "enabled", False))
    except Exception:
        tuning_enabled = False

    # model order: prefer tuning.models_order, else settings.models_order
    model_candidates = None
    try:
        model_candidates = list(getattr(getattr(settings, "tuning", None), "models_order", None) or [])
    except Exception:
        model_candidates = None
    if not model_candidates:
        try:
            model_candidates = list(getattr(settings, "models_order", None) or [])
        except Exception:
            model_candidates = []
    if not model_candidates:
        model_candidates = ["lightgbm", "ridge", "logreg", "random_forest"]

    if fast:
        model_candidates = [m for m in model_candidates if m in {"ridge", "logreg", "lightgbm", "random_forest", "extra_trees"}]

    # iterate horizons present in y_dict (y_reg_ or y_cls_)
    horizons = sorted({str(k).split("_")[-1] for k in y_dict.keys()})
    results: List[TrainResult] = []

    def _positive_proba(model, X_in: np.ndarray) -> np.ndarray:
        try:
            probs = model.predict_proba(X_in)
            probs = np.asarray(probs)
            if probs.ndim == 1:
                return probs
            if probs.shape[1] == 1:
                return probs[:, 0]
            return probs[:, 1]
        except Exception:
            # fallback: decision_function -> sigmoid
            try:
                scores = model.decision_function(X_in)
                scores = np.asarray(scores)
                return 1.0 / (1.0 + np.exp(-scores))
            except Exception:
                # last resort: map predicted class to {0,1}
                try:
                    pred = np.asarray(model.predict(X_in)).astype(float)
                    return np.clip(pred, 0.0, 1.0)
                except Exception:
                    return np.zeros(len(X_in), dtype=float)

    for h in _tqdm(horizons, desc="horizons"):
        y_reg_k = f"y_reg_{h}"
        y_cls_k = f"y_cls_{h}"
        task_items: List[tuple[str, pd.Series]] = []
        if y_cls_k in y_dict:
            task_items.append(("cls", y_dict[y_cls_k]))
        if y_reg_k in y_dict:
            task_items.append(("reg", y_dict[y_reg_k]))
        if not task_items:
            continue

        for task, y_full_raw in task_items:
            # align y to X index (defensive)
            y_full = y_full_raw.reindex(X.index)
            oof_index = y_full.index

            trained_any_model = False
            for model_name in _tqdm(model_candidates, desc=f"models:{h}:{task}", leave=False):
                # Task/model compatibility
                if model_name == "logreg" and task != "cls":
                    continue
                if model_name == "ridge" and task != "reg":
                    continue

                oof = np.full(len(y_full), np.nan, dtype=float)
                fold_metrics: List[FoldMetric] = []
                feature_importance: Optional[Dict[str, float]] = None
                device_used = "cpu"

                # Record effective hyperparameters used for this model/horizon/task.
                # This is critical for reproducibility checks (e.g., nondeterminism hard-kill).
                model_params: Dict[str, Any] = {}

                tuned_params = None

                ok_folds = 0
                for i, fold in enumerate(_tqdm(splits, desc=f"folds:{model_name}:{h}", leave=False)):
                    train_idx = np.asarray(getattr(fold, "train_idx", []), dtype=int)
                    val_idx = np.asarray(getattr(fold, "val_idx", []), dtype=int)

                    if train_idx.size == 0 or val_idx.size == 0:
                        continue

                    y_tr_full = y_full.iloc[train_idx]
                    y_val_full = y_full.iloc[val_idx]
                    tr_keep = ~y_tr_full.isna().to_numpy()
                    va_keep = ~y_val_full.isna().to_numpy()
                    tr_idx = train_idx[tr_keep]
                    va_idx = val_idx[va_keep]

                    if tr_idx.size < 50 or va_idx.size < 20:
                        continue

                    X_tr_df = X.iloc[tr_idx]
                    X_val_df = X.iloc[va_idx]
                    y_tr = y_full.iloc[tr_idx]
                    y_val = y_full.iloc[va_idx]

                    # Optional Optuna tuning (real data, walk-forward splits).
                    # Tune once per model/horizon/task to control runtime.
                    if tuned_params is None and tuning_enabled and model_name in {"lightgbm", "xgboost", "catboost", "hgb"}:
                        try:
                            if i == 0:
                                # Build a leakage-safe tuning dataset without NaN targets,
                                # and remap split indices to the compacted dataset.
                                y_mask = ~y_full.isna().to_numpy()
                                if int(y_mask.sum()) > 200:
                                    pos_map = -np.ones(len(y_mask), dtype=int)
                                    pos_map[np.where(y_mask)[0]] = np.arange(int(y_mask.sum()))
                                    remapped: list[SplitFold] = []
                                    for f in (splits[: min(3, len(splits))] if splits else []):
                                        tr = pos_map[np.asarray(getattr(f, 'train_idx', []), dtype=int)]
                                        va = pos_map[np.asarray(getattr(f, 'val_idx', []), dtype=int)]
                                        tr = tr[tr >= 0]
                                        va = va[va >= 0]
                                        if tr.size < 50 or va.size < 20:
                                            continue
                                        remapped.append(SplitFold(train_idx=tr.tolist(), val_idx=va.tolist()))
                                    if remapped:
                                        X_tune = X.loc[y_mask].fillna(0)
                                        y_tune = y_full.loc[y_mask]
                                        tuned_params, _, _ = tune_model(
                                            model_name,
                                            X_tune,
                                            y_tune,
                                            remapped,
                                            settings,
                                            device_profile,
                                            direction="maximize",
                                        )
                        except Exception:
                            tuned_params = None

                    # scale for linear models
                    if model_name in {"logreg", "ridge"} and scale_linear:
                        scaler = StandardScaler()
                        X_tr_m = scaler.fit_transform(X_tr_df.fillna(0))
                        X_val_m = scaler.transform(X_val_df.fillna(0))
                    else:
                        X_tr_m = X_tr_df.fillna(0).to_numpy()
                        X_val_m = X_val_df.fillna(0).to_numpy()

                    try:
                        if model_name == "logreg":
                            model = LogisticRegression(random_state=seed, max_iter=200)
                            model_params = {"random_state": seed, "max_iter": 200}
                            model.fit(X_tr_m, y_tr)
                            prob = _positive_proba(model, X_val_m)
                            pred = (prob > 0.5).astype(int)
                            try:
                                feature_importance = dict(zip(X.columns, np.abs(model.coef_.ravel()).tolist(), strict=False))
                            except Exception:
                                feature_importance = None
                        elif model_name == "ridge":
                            model = Ridge()
                            model_params = {}
                            model.fit(X_tr_m, y_tr)
                            pred = model.predict(X_val_m)
                            prob = pred
                            try:
                                feature_importance = dict(zip(X.columns, np.abs(model.coef_.ravel()).tolist(), strict=False))
                            except Exception:
                                feature_importance = None
                        elif model_name == "random_forest":
                            if task == "cls":
                                model = RandomForestClassifier(random_state=seed)
                                model_params = {"random_state": seed}
                                model.fit(X_tr_df.fillna(0), y_tr)
                                prob = _positive_proba(model, X_val_df.fillna(0).to_numpy())
                                pred = (prob > 0.5).astype(int)
                            else:
                                model = RandomForestRegressor(random_state=seed)
                                model_params = {"random_state": seed}
                                model.fit(X_tr_df.fillna(0), y_tr)
                                pred = model.predict(X_val_df.fillna(0))
                                prob = pred
                            try:
                                feature_importance = dict(zip(X.columns, getattr(model, "feature_importances_", []), strict=False))
                            except Exception:
                                feature_importance = None
                        elif model_name == "extra_trees":
                            if task == "cls":
                                model = ExtraTreesClassifier(random_state=seed)
                                model_params = {"random_state": seed}
                                model.fit(X_tr_df.fillna(0), y_tr)
                                prob = _positive_proba(model, X_val_df.fillna(0).to_numpy())
                                pred = (prob > 0.5).astype(int)
                            else:
                                model = ExtraTreesRegressor(random_state=seed)
                                model_params = {"random_state": seed}
                                model.fit(X_tr_df.fillna(0), y_tr)
                                pred = model.predict(X_val_df.fillna(0))
                                prob = pred
                            try:
                                feature_importance = dict(zip(X.columns, getattr(model, "feature_importances_", []), strict=False))
                            except Exception:
                                feature_importance = None
                        elif model_name == "xgboost":
                            try:
                                import xgboost as xgb
                            except Exception:
                                continue
                            params = getattr(settings, "xgb_params", {}) or {}
                            params = dict(params)
                            if tuned_params and isinstance(tuned_params, dict):
                                params.update(tuned_params)
                            # Merge device-safe params (do not overwrite objective/eval_metric).
                            try:
                                dev_params = safe_set_xgb_params(bool(getattr(settings, 'prefer_gpu', False)), device_profile) or {}
                                if isinstance(dev_params, dict):
                                    params.update(dev_params)
                            except Exception:
                                pass
                            # Ensure sane defaults if not configured.
                            if task == "cls":
                                params.setdefault("objective", "binary:logistic")
                                params.setdefault("eval_metric", "auc")
                            else:
                                params.setdefault("objective", "reg:squarederror")
                                params.setdefault("eval_metric", "rmse")
                            params.setdefault("seed", seed)
                            # Determinism: keep single-thread by default unless explicitly set.
                            params.setdefault("nthread", 1)
                            params.setdefault("random_state", seed)
                            model_params = dict(params)
                            dtrain = xgb.DMatrix(X_tr_df.fillna(0), label=y_tr)
                            dval = xgb.DMatrix(X_val_df.fillna(0), label=y_val)
                            evallist = [(dval, 'eval')]
                            bst = xgb.train(
                                params,
                                dtrain,
                                num_boost_round=int(getattr(settings, "num_boost_round", 300)),
                                evals=evallist,
                                early_stopping_rounds=int(
                                    getattr(
                                        settings,
                                        "early_stopping_rounds",
                                        getattr(settings, "early_stop_rounds", 30),
                                    )
                                ),
                                verbose_eval=False,
                            )
                            pred = bst.predict(dval)
                            prob = pred
                            device_used = "gpu" if str(params.get("tree_method", "")).startswith("gpu") else "cpu"
                        elif model_name == "lightgbm":
                            try:
                                import lightgbm as lgb
                            except Exception:
                                continue
                            params = getattr(settings, "lgbm_params", {}) or {}
                            params = dict(params)
                            if tuned_params and isinstance(tuned_params, dict):
                                params.update(tuned_params)
                            try:
                                dev_params = safe_set_lgbm_params(bool(getattr(settings, 'prefer_gpu', False)), device_profile) or {}
                                if isinstance(dev_params, dict):
                                    params.update(dev_params)
                            except Exception:
                                pass
                            if task == "cls":
                                params.setdefault("objective", "binary")
                                params.setdefault("metric", "auc")
                            else:
                                params.setdefault("objective", "regression")
                                params.setdefault("metric", "rmse")
                            params.setdefault("verbosity", -1)
                            # Determinism: LightGBM can be non-deterministic without explicit seeds
                            # and with multi-threaded training. For HF hard-kill reproducibility we
                            # default to deterministic training unless the user overrides.
                            params.setdefault("seed", seed)
                            params.setdefault("feature_fraction_seed", seed)
                            params.setdefault("bagging_seed", seed)
                            params.setdefault("data_random_seed", seed)
                            params.setdefault("drop_seed", seed)
                            params.setdefault("deterministic", True)
                            params.setdefault("num_threads", 1)
                            # Avoid ambiguous auto-parallel strategy.
                            params.setdefault("force_row_wise", True)

                            model_params = dict(params)

                            dtrain = lgb.Dataset(X_tr_df.fillna(0), label=y_tr)
                            dval = lgb.Dataset(X_val_df.fillna(0), label=y_val, reference=dtrain)
                            bst = lgb.train(
                                params,
                                dtrain,
                                num_boost_round=int(getattr(settings, "num_boost_round", 300)),
                                valid_sets=[dval],
                                callbacks=[
                                    lgb.early_stopping(
                                        int(
                                            getattr(
                                                settings,
                                                "early_stopping_rounds",
                                                getattr(settings, "early_stop_rounds", 30),
                                            )
                                        ),
                                        verbose=False,
                                    )
                                ],
                            )
                            pred = bst.predict(X_val_df.fillna(0))
                            prob = pred
                            device_used = str(params.get("device", "cpu"))
                        elif model_name == "hgb":
                            # sklearn HistGradientBoosting (CPU)
                            if task == "cls":
                                model = HistGradientBoostingClassifier(random_state=seed)
                                model_params = {"random_state": seed}
                                model.fit(X_tr_df.fillna(0), y_tr)
                                prob = model.predict_proba(X_val_df.fillna(0))[:, 1]
                                pred = (prob > 0.5).astype(int)
                            else:
                                model = HistGradientBoostingRegressor(random_state=seed)
                                model_params = {"random_state": seed}
                                model.fit(X_tr_df.fillna(0), y_tr)
                                pred = model.predict(X_val_df.fillna(0))
                                prob = pred
                        elif model_name == "catboost":
                            try:
                                from catboost import (
                                    CatBoostClassifier,
                                    CatBoostRegressor,
                                )
                            except Exception:
                                continue
                            params = getattr(settings, "cat_params", {}) or {}
                            params = dict(params)
                            # Merge device-safe params without overwriting configured loss.
                            try:
                                dev_params = safe_set_cat_params(bool(getattr(settings, 'prefer_gpu', False)), device_profile) or {}
                                if isinstance(dev_params, dict):
                                    params.update(dev_params)
                            except Exception:
                                pass
                            params.setdefault("random_seed", seed)
                            model_params = dict(params)
                            if task == "cls":
                                params.setdefault("loss_function", "Logloss")
                                model = CatBoostClassifier(**params)
                                model.fit(X_tr_df.fillna(0), y_tr, eval_set=(X_val_df.fillna(0), y_val), verbose=False)
                                prob = _positive_proba(model, X_val_df.fillna(0).to_numpy())
                                pred = (prob > 0.5).astype(int)
                            else:
                                params.setdefault("loss_function", "RMSE")
                                model = CatBoostRegressor(**params)
                                model.fit(X_tr_df.fillna(0), y_tr, eval_set=(X_val_df.fillna(0), y_val), verbose=False)
                                pred = model.predict(X_val_df.fillna(0))
                                prob = pred
                            device_used = "gpu" if str(params.get("task_type", "")).upper() == "GPU" else "cpu"
                        else:
                            continue

                        # store OOF predictions for valid val indices
                        try:
                            oof[va_idx] = np.asarray(prob, dtype=float)
                        except Exception:
                            # fall back to length-based assignment
                            if len(va_idx) == len(prob):
                                oof[va_idx] = prob

                        # Base predictive metrics
                        if task == "cls":
                            m: Dict[str, float] = _metrics_cls(y_val.values, prob, pred)
                        else:
                            m = _metrics_reg(y_val.values, pred)

                        # Optional: fold-level strategy metrics (OOS and IS) for institutional gates
                        if compute_strategy_metrics:
                            try:
                                # OOS (validation) strategy metrics
                                p_val = prices
                                s_val = pd.Series(np.asarray(prob if task == "cls" else pred, dtype=float), index=y_val.index)
                                out_val = compute_equity_and_metrics(p_val, s_val, eval_settings)
                                mm_val = out_val.get('metrics')
                                m['sharpe'] = float(getattr(mm_val, 'sharpe', float('nan')))
                                m['max_drawdown'] = float(getattr(mm_val, 'max_drawdown', float('nan')))
                                m['n_trades'] = float(getattr(mm_val, 'n_trades', float('nan')))
                            except Exception:
                                pass
                            try:
                                # IS (training) strategy Sharpe
                                if task == "cls":
                                    prob_tr = _positive_proba(model, X_tr_m)
                                    s_tr = pd.Series(np.asarray(prob_tr, dtype=float), index=y_tr.index)
                                else:
                                    pred_tr = np.asarray(model.predict(X_tr_m)).astype(float)
                                    s_tr = pd.Series(pred_tr, index=y_tr.index)
                                p_tr = prices
                                out_tr = compute_equity_and_metrics(p_tr, s_tr, eval_settings)
                                mm_tr = out_tr.get('metrics')
                                v = getattr(mm_tr, 'sharpe', None)
                                m['sharpe_is'] = float(v) if v is not None else float('nan')
                            except Exception:
                                pass

                        fold_metrics.append(FoldMetric(fold=i, metric=m))

                        ok_folds += 1
                    except Exception:
                        # try next fold/model
                        continue

                if ok_folds == 0:
                    continue

                results.append(
                    TrainResult(
                        model_name=str(model_name),
                        task=str(task),
                        horizon=str(h),
                        fold_metrics=fold_metrics,
                        oof_predictions={"index": [str(x) for x in oof_index.tolist()], "pred": oof.tolist()},
                        feature_importance=feature_importance,
                        params=model_params,
                        device_used=device_used,
                    )
                )
                trained_any_model = True

                # In non-fast mode, keep evaluating additional models.
                # In fast mode, stop after the first successful model to control runtime.
                if fast:
                    break

            if not trained_any_model:
                continue

    return results
