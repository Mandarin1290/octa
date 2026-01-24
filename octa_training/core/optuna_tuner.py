from __future__ import annotations

import logging
import traceback
from typing import Any, Dict, List

import numpy as np

try:
    import optuna
    from optuna.samplers import TPESampler
except Exception:
    optuna = None

# metrics helpers
try:
    from sklearn.metrics import log_loss, mean_squared_error
except Exception:
    # metrics may not be available in constrained envs; functions will catch missing imports at runtime
    log_loss = None  # type: ignore
    mean_squared_error = None  # type: ignore

from octa_training.core.splits import SplitFold


def _lgbm_space(trial: Any) -> Dict[str, Any]:
    return {
        "learning_rate": trial.suggest_loguniform("learning_rate", 1e-3, 0.3),
        "num_leaves": trial.suggest_int("num_leaves", 16, 256),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.3, 1.0),
        "reg_lambda": trial.suggest_loguniform("reg_lambda", 1e-3, 10.0),
    }


def _bounded_space(trial: Any, model_name: str, cfg) -> Dict[str, Any]:
    """Return bounded Optuna space from cfg.tuning.search_space if present."""
    try:
        space = getattr(getattr(cfg, 'tuning', None), 'search_space', None)
        if not isinstance(space, dict) or not space:
            return {}
        ms = space.get(model_name)
        if not isinstance(ms, dict) or not ms:
            return {}
    except Exception:
        return {}

    params: Dict[str, Any] = {}
    for pname, spec in ms.items():
        if not isinstance(spec, dict):
            continue
        ptype = str(spec.get('type') or '').strip().lower()
        low = spec.get('low')
        high = spec.get('high')
        if low is None or high is None:
            continue
        try:
            if ptype in {'log_float', 'loguniform'}:
                params[pname] = trial.suggest_float(pname, float(low), float(high), log=True)
            elif ptype in {'float', 'uniform'}:
                params[pname] = trial.suggest_float(pname, float(low), float(high))
            elif ptype in {'int'}:
                params[pname] = trial.suggest_int(pname, int(low), int(high))
        except Exception:
            continue
    return params


def _xgb_space(trial: Any) -> Dict[str, Any]:
    return {
        "eta": trial.suggest_loguniform("eta", 1e-3, 0.3),
        "max_depth": trial.suggest_int("max_depth", 3, 12),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.3, 1.0),
        "lambda": trial.suggest_loguniform("lambda", 1e-3, 10.0),
    }


def _cat_space(trial: Any) -> Dict[str, Any]:
    return {
        "learning_rate": trial.suggest_loguniform("learning_rate", 1e-3, 0.3),
        "depth": trial.suggest_int("depth", 4, 10),
        "l2_leaf_reg": trial.suggest_loguniform("l2_leaf_reg", 1e-3, 10.0),
        "border_count": trial.suggest_int("border_count", 32, 255),
    }


def _hgb_space(trial: Any) -> Dict[str, Any]:
    return {
        "learning_rate": trial.suggest_loguniform("hgb_learning_rate", 1e-4, 0.2),
        "max_iter": trial.suggest_int("hgb_max_iter", 50, 500),
        "max_leaf_nodes": trial.suggest_int("hgb_max_leaf_nodes", 15, 255),
        "min_samples_leaf": trial.suggest_int("hgb_min_samples_leaf", 1, 50),
    }


def tune_model(model_name: str, X, y, splits: List[SplitFold], cfg, device_profile, direction: str = "maximize") -> Dict[str, Any]:
    """Tune primary model hyperparameters using optuna. Returns best_params dict.

    model_name: one of 'lightgbm','xgboost','catboost'
    direction: 'maximize' or 'minimize'
    """
    if optuna is None:
        raise RuntimeError("optuna not installed; install optuna to enable tuning")

    logger = logging.getLogger("octa_training")
    trials = int(cfg.tuning.optuna_trials if hasattr(cfg, 'tuning') else 50)
    timeout = None
    raw_timeout = getattr(cfg.tuning, 'timeout_sec', None) if hasattr(cfg, 'tuning') else None
    try:
        if raw_timeout is not None and str(raw_timeout).strip() != "":
            raw_timeout_val = float(raw_timeout)
            if raw_timeout_val > 0:
                timeout = int(raw_timeout_val)
    except Exception:
        timeout = None
    seed = int(getattr(cfg, 'seed', 42))

    sampler = TPESampler(seed=seed)
    # Enable Optuna pruning (early termination of bad trials) to reduce runtime.
    # This does not change data usage (no simulation), it only stops clearly bad hyperparams sooner.
    pruner = None
    try:
        pruner = optuna.pruners.MedianPruner(n_startup_trials=10, n_warmup_steps=1)
    except Exception:
        pruner = None
    study = optuna.create_study(direction=direction, sampler=sampler, pruner=pruner)

    def objective(trial):
        try:
            # Prefer bounded ranges from config (HF anti-overfit). Fallback to wider defaults.
            params = _bounded_space(trial, model_name, cfg)
            if not params:
                if model_name == 'lightgbm':
                    params = _lgbm_space(trial)
                elif model_name == 'hgb':
                    params = _hgb_space(trial)
                elif model_name == 'xgboost':
                    params = _xgb_space(trial)
                elif model_name == 'catboost':
                    params = _cat_space(trial)
                else:
                    raise ValueError('Unsupported model for tuning')

            # simple CV over provided splits (no leakage)
            scores = []
            for fold_i, fold in enumerate(splits):
                train_idx = fold.train_idx
                val_idx = fold.val_idx
                X_tr = X.iloc[train_idx]
                X_val = X.iloc[val_idx]
                y_tr = y.iloc[train_idx]
                y_val = y.iloc[val_idx]
                if X_tr.shape[0] < 2 or X_val.shape[0] < 1:
                    return 1e9 if direction == 'minimize' else -1e9

                if model_name == 'lightgbm':
                    import lightgbm as lgb
                    p = params.copy()
                    p.setdefault('objective', 'binary' if set(y.unique()) <= {0,1} else 'regression')
                    p.setdefault('verbosity', -1)
                    dtrain = lgb.Dataset(X_tr, label=y_tr)
                    dval = lgb.Dataset(X_val, label=y_val, reference=dtrain)
                    bst = lgb.train(p, dtrain, num_boost_round=200, valid_sets=[dval], early_stopping_rounds=cfg.tuning.early_stop_rounds)
                    pred = bst.predict(X_val)
                    if set(y.unique()) <= {0,1}:
                        score = float(-log_loss(y_val, pred)) if direction=='maximize' else float(mean_squared_error(y_val, pred))
                    else:
                        score = float(-mean_squared_error(y_val, pred)) if direction=='maximize' else float(mean_squared_error(y_val, pred))
                elif model_name == 'xgboost':
                    import xgboost as xgb
                    p = params.copy()
                    dtrain = xgb.DMatrix(X_tr, label=y_tr)
                    dval = xgb.DMatrix(X_val, label=y_val)
                    bst = xgb.train(p, dtrain, num_boost_round=200, evals=[(dval,'eval')], early_stopping_rounds=cfg.tuning.early_stop_rounds)
                    pred = bst.predict(dval)
                    # If binary classification, tune on logloss (maximize -logloss).
                    if set(y.unique()) <= {0, 1} and log_loss is not None:
                        score = float(-log_loss(y_val, pred)) if direction == 'maximize' else float(mean_squared_error(y_val, pred))
                    else:
                        score = float(-mean_squared_error(y_val, pred)) if direction=='maximize' else float(mean_squared_error(y_val, pred))
                elif model_name == 'catboost':
                    from catboost import CatBoost
                    p = params.copy()
                    cb = CatBoost(**{**p, 'iterations':200, 'verbose':False})
                    cb.fit(X_tr, y_tr, eval_set=(X_val, y_val), early_stopping_rounds=cfg.tuning.early_stop_rounds, verbose=False)
                    pred = cb.predict(X_val)
                    if set(y.unique()) <= {0, 1} and log_loss is not None:
                        # CatBoost returns class labels by default; use probability if available.
                        try:
                            pred = cb.predict_proba(X_val)[:, 1]
                        except Exception:
                            pass
                        score = float(-log_loss(y_val, pred)) if direction == 'maximize' else float(mean_squared_error(y_val, pred))
                    else:
                        score = float(-mean_squared_error(y_val, pred)) if direction=='maximize' else float(mean_squared_error(y_val, pred))
                elif model_name == 'hgb':
                    # tune sklearn HistGradientBoosting
                    from sklearn.ensemble import (
                        HistGradientBoostingClassifier,
                        HistGradientBoostingRegressor,
                    )
                    p = params.copy()
                    # Support both bounded (clean names) and legacy-prefixed param names.
                    lr = p.get('learning_rate', p.get('hgb_learning_rate', 0.05))
                    mi = p.get('max_iter', p.get('hgb_max_iter', 200))
                    mln = p.get('max_leaf_nodes', p.get('hgb_max_leaf_nodes', None))
                    msl = p.get('min_samples_leaf', p.get('hgb_min_samples_leaf', 20))
                    if set(y.unique()) <= {0, 1}:
                        clf = HistGradientBoostingClassifier(learning_rate=lr, max_iter=mi, max_leaf_nodes=mln, min_samples_leaf=msl, random_state=seed)
                        clf.fit(X_tr, y_tr)
                        pred = clf.predict_proba(X_val)[:, 1]
                        if direction == 'maximize':
                            score = float(-log_loss(y_val, pred))
                        else:
                            score = float(mean_squared_error(y_val, pred))
                    else:
                        clf = HistGradientBoostingRegressor(learning_rate=lr, max_iter=mi, max_leaf_nodes=mln, min_samples_leaf=msl, random_state=seed)
                        clf.fit(X_tr, y_tr)
                        pred = clf.predict(X_val)
                        score = float(-mean_squared_error(y_val, pred)) if direction == 'maximize' else float(mean_squared_error(y_val, pred))
                else:
                    raise ValueError('unsupported')
                scores.append(score)

                # Report intermediate score for pruner.
                try:
                    # Higher is better in our internal convention (we negate loss for maximize).
                    trial.report(float(np.mean(scores)), step=int(fold_i))
                    if trial.should_prune():
                        raise optuna.TrialPruned()
                except Exception:
                    pass

            mean_score = float(np.mean(scores))
            # penalize instability
            std_score = float(np.std(scores))
            # simplicity bias: softly penalize excessive model complexity
            complexity = 0.0
            try:
                if model_name == 'lightgbm':
                    complexity += float(params.get('num_leaves', 0)) / 128.0
                    complexity += float(params.get('min_child_samples', 0)) / 200.0
                elif model_name == 'xgboost':
                    complexity += float(params.get('max_depth', 0)) / 12.0
                elif model_name == 'catboost':
                    complexity += float(params.get('depth', 0)) / 10.0
                elif model_name == 'hgb':
                    complexity += float(params.get('max_leaf_nodes', params.get('hgb_max_leaf_nodes', 0))) / 255.0
                    complexity += float(params.get('max_iter', params.get('hgb_max_iter', 0))) / 500.0
            except Exception:
                complexity = 0.0

            penalized = mean_score - 0.1 * std_score - 0.02 * complexity
            return penalized
        except Exception:
            traceback.print_exc()
            return 1e9 if direction=='minimize' else -1e9

    study.optimize(objective, n_trials=trials, timeout=timeout)
    try:
        total_trials = len(study.trials)
        completed = len([t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE])
        pruned = len([t for t in study.trials if t.state == optuna.trial.TrialState.PRUNED])
        failed = len([t for t in study.trials if t.state == optuna.trial.TrialState.FAIL])
        if timeout is None:
            stop_reason = "n_trials"
        else:
            stop_reason = "n_trials" if total_trials >= trials else "timeout"
        logger.info(
            "Optuna tuning stop_reason=%s total=%d completed=%d pruned=%d failed=%d n_trials=%d timeout_sec=%s",
            stop_reason,
            total_trials,
            completed,
            pruned,
            failed,
            trials,
            str(timeout) if timeout is not None else "null",
        )
    except Exception:
        pass
    return study.best_params, study.best_value, study.trials_dataframe()
