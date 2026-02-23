"""Tests for deterministic data sanitization."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from octa.core.data.quality.sanitizer import (
    SEVERITY_INFO,
    SEVERITY_SEVERE,
    SEVERITY_WARNING,
    SanitizationResult,
    sanitize_series,
)


def _make_clean_df(n: int = 100) -> pd.DataFrame:
    rng = np.random.RandomState(42)
    dates = pd.date_range("2024-01-01", periods=n, freq="1D")
    prices = 100.0 + rng.randn(n).cumsum() * 0.5
    return pd.DataFrame({"close": prices}, index=dates)


def test_clean_series_passes() -> None:
    df = _make_clean_df()
    result = sanitize_series(df, symbol="AAPL", timeframe="1D")
    assert result.ok is True
    assert result.symbol == "AAPL"
    assert result.timeframe == "1D"
    severe = [f for f in result.flags if f.severity == SEVERITY_SEVERE]
    assert len(severe) == 0


def test_empty_dataframe_severe() -> None:
    result = sanitize_series(pd.DataFrame(), symbol="BAD", timeframe="1D")
    assert result.ok is False
    assert any(f.check == "empty_data" for f in result.flags)


def test_duplicate_timestamps_warning() -> None:
    df = _make_clean_df(50)
    # Create duplicates
    idx = df.index.tolist()
    idx[5] = idx[4]  # duplicate
    df.index = pd.DatetimeIndex(idx)
    result = sanitize_series(df, symbol="DUP", timeframe="1D")
    dup_flags = [f for f in result.flags if f.check == "duplicate_timestamps"]
    assert len(dup_flags) == 1
    assert dup_flags[0].severity in {SEVERITY_WARNING, SEVERITY_SEVERE}


def test_duplicate_timestamps_severe_many() -> None:
    df = _make_clean_df(50)
    idx = df.index.tolist()
    for i in range(1, 10):
        idx[i] = idx[0]  # many duplicates
    df.index = pd.DatetimeIndex(idx)
    result = sanitize_series(df, symbol="MANYDUP", timeframe="1D")
    dup_flags = [f for f in result.flags if f.check == "duplicate_timestamps"]
    assert dup_flags[0].severity == SEVERITY_SEVERE


def test_non_monotonic_index_severe() -> None:
    df = _make_clean_df(50)
    idx = df.index.tolist()
    idx[10], idx[11] = idx[11], idx[10]  # swap
    df.index = pd.DatetimeIndex(idx)
    result = sanitize_series(df, symbol="NONMONO", timeframe="1D")
    assert result.ok is False
    assert any(f.check == "non_monotonic_index" for f in result.flags)


def test_excessive_nans_severe() -> None:
    df = _make_clean_df(100)
    df.loc[df.index[:30], "close"] = np.nan  # 30% NaN
    result = sanitize_series(df, symbol="NAN", timeframe="1D", max_nan_frac=0.20)
    assert result.ok is False
    nan_flags = [f for f in result.flags if "nan_excessive" in f.check]
    assert len(nan_flags) >= 1


def test_minor_nans_info() -> None:
    df = _make_clean_df(100)
    df.loc[df.index[:2], "close"] = np.nan  # 2% NaN
    result = sanitize_series(df, symbol="MINNAN", timeframe="1D")
    assert result.ok is True
    nan_flags = [f for f in result.flags if "nan_present" in f.check]
    assert len(nan_flags) >= 1
    assert nan_flags[0].severity == SEVERITY_INFO


def test_spike_detection() -> None:
    df = _make_clean_df(200)
    # Inject a massive spike
    df.iloc[100, df.columns.get_loc("close")] = df.iloc[99, df.columns.get_loc("close")] * 3.0
    result = sanitize_series(
        df, symbol="SPIKE", timeframe="1D",
        spike_zscore_threshold=4.0, spike_window=20,
    )
    spike_flags = [f for f in result.flags if f.check == "price_spikes"]
    assert len(spike_flags) >= 1


def test_gap_detection() -> None:
    dates = pd.date_range("2024-01-01", periods=100, freq="1D")
    # Remove 20 dates to create gaps
    keep = [i for i in range(100) if i % 5 != 0]
    df = pd.DataFrame(
        {"close": np.arange(len(keep), dtype=float) + 100.0},
        index=dates[keep],
    )
    result = sanitize_series(
        df, symbol="GAPPY", timeframe="1D",
        expected_freq="1D", max_gap_frac=0.05,
    )
    gap_flags = [f for f in result.flags if f.check == "excessive_gaps"]
    assert len(gap_flags) >= 1


def test_no_gaps_within_threshold() -> None:
    df = _make_clean_df(100)
    result = sanitize_series(
        df, symbol="OK", timeframe="1D",
        expected_freq="1D", max_gap_frac=0.10,
    )
    gap_flags = [f for f in result.flags if f.check == "excessive_gaps"]
    assert len(gap_flags) == 0


def test_deterministic_output() -> None:
    df = _make_clean_df(100)
    r1 = sanitize_series(df, symbol="DET", timeframe="1D")
    r2 = sanitize_series(df, symbol="DET", timeframe="1D")
    assert r1.ok == r2.ok
    assert len(r1.flags) == len(r2.flags)
    assert r1.stats == r2.stats


def test_non_datetime_index_severe() -> None:
    df = pd.DataFrame({"close": [1.0, 2.0, 3.0]}, index=[0, 1, 2])
    result = sanitize_series(df, symbol="BADIDX", timeframe="1D")
    assert result.ok is False
    assert any(f.check == "non_datetime_index" for f in result.flags)
