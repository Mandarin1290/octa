from __future__ import annotations

import random
from typing import Callable, List, Tuple

import numpy as np


def kfold_split(n: int, k: int, seed: int | None = None) -> List[Tuple[List[int], List[int]]]:
    idx = list(range(n))
    if seed is not None:
        random.Random(seed).shuffle(idx)
    else:
        random.shuffle(idx)
    folds = []
    size = n // k
    for i in range(k):
        start = i * size
        end = start + size if i < k - 1 else n
        val = idx[start:end]
        train = [x for x in idx if x not in val]
        folds.append((train, val))
    return folds


def cross_validate(model_factory: Callable[[], object], X: List[list], y: List[float], k: int = 5, seed: int | None = None) -> dict:
    n = len(X)
    if n == 0:
        return {"error": "empty dataset"}
    folds = kfold_split(n, k, seed=seed)
    mses = []
    maes = []
    for train_idx, val_idx in folds:
        X_train = [X[i] for i in train_idx]
        y_train = [y[i] for i in train_idx]
        X_val = [X[i] for i in val_idx]
        y_val = [y[i] for i in val_idx]
        m = model_factory()
        m.fit(X_train, y_train)
        preds = m.predict(X_val)
        preds = [float(p) for p in preds]
        yv = [float(v) for v in y_val]
        mse = float(np.mean([(a - b) ** 2 for a, b in zip(preds, yv, strict=False)])) if preds else 0.0
        mae = float(np.mean([abs(a - b) for a, b in zip(preds, yv, strict=False)])) if preds else 0.0
        mses.append(mse)
        maes.append(mae)
    return {"mse_mean": float(np.mean(mses)), "mse_std": float(np.std(mses)), "mae_mean": float(np.mean(maes)), "mae_std": float(np.std(maes))}
