from __future__ import annotations

from typing import Any, Callable, Dict, List

from scripts.cross_validation import cross_validate


def grid_search_feature_count(X, y, k_choices: List[int], seed: int = 42) -> Dict[str, Any]:
    """Legacy grid search kept for backward compatibility."""
    import numpy as np

    def _default_factory():
        try:
            from scripts.train_and_save import make_estimator

            return make_estimator(None)
        except Exception:
            # final fallback: a very simple ridge regressor
            from sklearn.linear_model import Ridge

            return Ridge(alpha=1.0)

    if not X:
        return {"error": "empty X"}
    arr = np.array(X)
    if arr.ndim == 1:
        variances = [0.0]
    else:
        variances = arr.var(axis=0).tolist()
    cols_idx = list(range(arr.shape[1]))
    ranked = sorted(cols_idx, key=lambda i: variances[i], reverse=True)

    best = None
    best_score = float("inf")
    details = {}
    for k in k_choices:
        keep = ranked[:k]
        X_sub = [[row[i] for i in keep] for row in X]
        res = cross_validate(_default_factory, X_sub, y, k=3, seed=seed)
        details[k] = res
        if "mse_mean" in res and res["mse_mean"] < best_score:
            best_score = res["mse_mean"]
            best = {"feature_count": k, "cv": res, "keep_idx": keep}

    return {"best": best, "details": details}


def optuna_feature_search(model_factory: Callable[[], object], X, y, max_features: int, n_trials: int = 20, seed: int = 42) -> Dict[str, Any]:
    """Use Optuna to search for best number of top-variance features.

    The search space is the integer k in [1, max_features]. For each trial we select
    the top-k features by variance and evaluate CV. Returns best trial info.
    """
    try:
        import numpy as np
        import optuna
    except Exception as e:
        return {"error": "optuna not installed", "exc": str(e)}

    if not X:
        return {"error": "empty X"}
    arr = np.array(X)
    if arr.ndim == 1:
        variances = [0.0]
    else:
        variances = arr.var(axis=0).tolist()
    cols_idx = list(range(arr.shape[1]))
    ranked = sorted(cols_idx, key=lambda i: variances[i], reverse=True)

    def objective(trial: optuna.trial.Trial) -> float:
        k = trial.suggest_int("k", 1, max_features)
        keep = ranked[:k]
        X_sub = [[row[i] for i in keep] for row in X]
        res = cross_validate(model_factory, X_sub, y, k=3, seed=seed)
        # minimize mse_mean
        if "mse_mean" in res:
            return float(res["mse_mean"])
        return float("inf")

    sampler = optuna.samplers.TPESampler(seed=seed)
    study = optuna.create_study(direction="minimize", sampler=sampler)
    study.optimize(objective, n_trials=n_trials)

    best_k = int(study.best_params["k"])
    keep_idx = ranked[:best_k]
    best_cv = None
    # compute CV for best
    X_best = [[row[i] for i in keep_idx] for row in X]
    best_cv = cross_validate(model_factory, X_best, y, k=3, seed=seed)

    return {"best": {"feature_count": best_k, "keep_idx": keep_idx, "cv": best_cv}, "study": {"best_value": study.best_value, "n_trials": len(study.trials)}}
