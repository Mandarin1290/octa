from __future__ import annotations

import pandas as pd

from octa.core.research.validation.walk_forward import make_walk_forward_splits


def test_walk_forward_splits_basic() -> None:
    index = pd.date_range("2020-01-01", periods=400, freq="D")
    cfg = {
        "train_days": 200,
        "test_days": 50,
        "step_days": 50,
        "warmup_days": 0,
        "embargo_bars": 2,
        "timeframe": "1D",
    }
    splits = make_walk_forward_splits(index, cfg)
    assert splits
    for split in splits:
        assert split.train_idx[-1] < split.test_idx[0]
