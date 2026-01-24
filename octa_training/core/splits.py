from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import numpy as np
import pandas as pd


@dataclass
class SplitFold:
    train_idx: np.ndarray
    val_idx: np.ndarray
    fold_meta: Dict[str, Any]


def assert_no_overlap(train_idx: np.ndarray, val_idx: np.ndarray) -> bool:
    if np.intersect1d(train_idx, val_idx).size != 0:
        raise AssertionError("Train and validation indices overlap")
    return True


def walk_forward_splits(index: pd.Index, n_folds: int, train_window: int, test_window: int, step: int = 1, purge_size: int = 0, embargo_size: int = 0, min_train_size: int = 1, min_test_size: int = 1, expanding: bool = True, min_folds_required: int = 1) -> List[SplitFold]:
    """
    Generate deterministic walk-forward splits by bar counts.

    index: pandas Index (DatetimeIndex or otherwise)
    train_window/test_window/step/purge/embargo are in bars (integers)
    expanding: if True, train start fixed at 0 and train end expands; otherwise rolling window of train_window length
    Returns list of SplitFold; may skip folds not meeting min sizes.
    If effective folds < min_folds_required -> raise Exception.
    """
    n = len(index)
    if n == 0:
        return []

    folds: List[SplitFold] = []
    # compute test start positions: start at initial_train_end + 1
    # initial_train_end for expanding = train_window-1 else train_window-1
    # We'll sweep test_start from (train_window) to n-test_window step by step.
    test_start_positions = list(range(train_window, n - test_window + 1, step))

    # n_folds behavior:
    #  - n_folds > 0: take the first n_folds folds (legacy behavior)
    #  - n_folds < 0: take the LAST abs(n_folds) folds (gives recent coverage)
    if n_folds < 0:
        k = int(abs(n_folds))
        if k > 0:
            test_start_positions = test_start_positions[-k:]

    for test_start in test_start_positions:
        test_end = test_start + test_window - 1
        if expanding:
            train_start = 0
        else:
            train_start = max(0, test_start - train_window)
        train_end = test_start - 1

        # Build initial train and val index arrays
        train_idx = np.arange(train_start, train_end + 1)
        val_idx = np.arange(test_start, test_end + 1)

        # Purge: remove from train any indices within purge_size before test_start
        if purge_size > 0:
            purge_from = max(train_start, test_start - purge_size)
            mask = (train_idx < purge_from)
            train_idx = train_idx[mask]

        # Embargo: remove from train indices in (test_end+1 .. test_end+embargo_size)
        if embargo_size > 0:
            embargo_from = test_end + 1
            embargo_to = min(n - 1, test_end + embargo_size)
            mask = ~((train_idx >= embargo_from) & (train_idx <= embargo_to))
            train_idx = train_idx[mask]

        # Validate sizes
        if train_idx.size < min_train_size or val_idx.size < min_test_size:
            # skip this fold
            continue

        # Ensure determinism and no overlap
        assert_no_overlap(train_idx, val_idx)

        fold_meta = {
            "train_range": (int(train_idx[0]) if train_idx.size else None, int(train_idx[-1]) if train_idx.size else None),
            "val_range": (int(val_idx[0]), int(val_idx[-1])),
            "train_size": int(train_idx.size),
            "val_size": int(val_idx.size),
            "test_start_pos": int(test_start),
            "test_end_pos": int(test_end),
            "train_start_pos": int(train_start),
            "train_end_pos": int(train_end),
        }
        folds.append(SplitFold(train_idx=train_idx, val_idx=val_idx, fold_meta=fold_meta))
        if n_folds > 0 and len(folds) >= n_folds:
            break

    if len(folds) < min_folds_required:
        raise ValueError(f"Insufficient effective folds: requested {n_folds} min_required {min_folds_required} produced {len(folds)}")

    return folds


def describe_splits(splits: List[SplitFold], index: pd.Index) -> Dict[str, Any]:
    res = {"n_folds": len(splits), "folds": []}
    for i, f in enumerate(splits):
        train_idx = f.train_idx
        val_idx = f.val_idx
        train_dates = (str(index[train_idx[0]]) if train_idx.size else None, str(index[train_idx[-1]]) if train_idx.size else None)
        val_dates = (str(index[val_idx[0]]), str(index[val_idx[-1]]))
        res["folds"].append({"fold": i, "train_size": int(train_idx.size), "val_size": int(val_idx.size), "train_dates": train_dates, "val_dates": val_dates})
    return res
