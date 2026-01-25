from __future__ import annotations

import pandas as pd

from octa.core.research.validation.purged_cv import purged_kfold_splits


def test_purged_cv_no_overlap() -> None:
    index = pd.date_range("2020-01-01", periods=100, freq="D")
    splits = purged_kfold_splits(index, n_splits=5, embargo=2, purge=2)
    assert splits
    for split in splits:
        assert set(split.train_idx).isdisjoint(set(split.test_idx))
