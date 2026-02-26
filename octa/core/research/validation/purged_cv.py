from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd

from .walk_forward import Split


@dataclass(frozen=True)
class PurgedFold:
    train_idx: List[int]
    test_idx: List[int]
    meta: Dict[str, Any]


def purged_kfold_splits(
    index: pd.Index,
    n_splits: int,
    embargo: int,
    purge: int,
    mode: str = "pooled",
) -> List[Split]:
    if n_splits <= 1:
        return []
    n = len(index)
    if n == 0:
        return []
    fold_sizes = _fold_sizes(n, n_splits)
    boundaries = _fold_boundaries(fold_sizes)
    splits: List[Split] = []
    for fold, (start, end) in enumerate(boundaries):
        test_idx = list(range(start, end))
        train_idx = [i for i in range(0, start)] + [i for i in range(end, n)]
        if purge > 0:
            purge_start = max(0, start - purge)
            train_idx = [i for i in train_idx if i < purge_start or i >= end]
        if embargo > 0:
            embargo_end = min(n, end + embargo)
            train_idx = [i for i in train_idx if i < end or i >= embargo_end]
        if not train_idx or not test_idx:
            continue
        splits.append(
            Split(
                train_idx=train_idx,
                test_idx=test_idx,
                meta={
                    "fold": fold,
                    "test_start": start,
                    "test_end": end - 1,
                    "train_size": len(train_idx),
                    "test_size": len(test_idx),
                    "mode": mode,
                },
            )
        )
    return splits


def _fold_sizes(n: int, n_splits: int) -> List[int]:
    base = n // n_splits
    rem = n % n_splits
    sizes = [base] * n_splits
    for i in range(rem):
        sizes[i] += 1
    return sizes


def _fold_boundaries(fold_sizes: List[int]) -> List[tuple[int, int]]:
    out: List[tuple[int, int]] = []
    start = 0
    for sz in fold_sizes:
        end = start + sz
        out.append((start, end))
        start = end
    return out
