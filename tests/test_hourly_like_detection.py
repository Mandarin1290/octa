import pandas as pd

from scripts.train_stocks_pkl_1d_1h import _is_hourly_like_index


def test_hourly_like_detection_true_for_hourly_index() -> None:
    idx = pd.date_range("2024-01-01", periods=300, freq="1h", tz="UTC")
    assert _is_hourly_like_index(idx)


def test_hourly_like_detection_false_for_daily_index() -> None:
    idx = pd.date_range("2024-01-01", periods=300, freq="1D", tz="UTC")
    assert not _is_hourly_like_index(idx)
