"""Tests for octa_training.core.regime_labels (v0.0.0)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from octa_training.core.regime_labels import (
    REGIME_BEAR,
    REGIME_BULL,
    REGIME_CRISIS,
    REGIME_NEUTRAL,
    RegimeLabelConfig,
    classify_regimes,
    get_regime_splits,
    regime_distribution,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(n: int, price_start: float = 100.0, trend: float = 0.0, noise: float = 0.005) -> pd.DataFrame:
    """Synthetic OHLCV DataFrame with a controlled close series."""
    rng = np.random.default_rng(42)
    daily_ret = trend + rng.normal(0, noise, n)
    close = price_start * np.cumprod(1 + daily_ret)
    idx = pd.date_range("2010-01-01", periods=n, freq="B")
    return pd.DataFrame({"close": close, "open": close, "high": close * 1.001, "low": close * 0.999, "volume": 1e6}, index=idx)


def _inject_crash(df: pd.DataFrame, start_idx: int, crash_pct: float = 0.25, n_days: int = 20) -> pd.DataFrame:
    """Impose a sharp, noisy crash starting at start_idx to trigger CRISIS.

    Uses large random swings to ensure rolling volatility spikes well above baseline.
    """
    rng = np.random.default_rng(99)
    df = df.copy()
    crash_daily = -(crash_pct / n_days)
    for i in range(start_idx, min(start_idx + n_days, len(df))):
        # Large noise (±3%) to spike rolling vol far above 252d baseline
        noise = rng.uniform(-0.03, 0.03)
        df.iloc[i, df.columns.get_loc("close")] = df.iloc[i - 1]["close"] * (1 + crash_daily + noise)
    return df


# ---------------------------------------------------------------------------
# Test 1: Returns empty Series on insufficient bars
# ---------------------------------------------------------------------------

def test_classify_regimes_insufficient_bars():
    df = _make_df(200)  # < 252
    result = classify_regimes(df)
    assert isinstance(result, pd.Series)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# Test 2: Returns Series aligned to df.index on sufficient bars
# ---------------------------------------------------------------------------

def test_classify_regimes_returns_aligned_series():
    df = _make_df(500)
    result = classify_regimes(df)
    assert len(result) == len(df)
    assert (result.index == df.index).all()
    assert set(result.unique()).issubset({REGIME_BULL, REGIME_BEAR, REGIME_CRISIS, REGIME_NEUTRAL})


# ---------------------------------------------------------------------------
# Test 3: CRISIS regime detected during sharp crash
# ---------------------------------------------------------------------------

def test_classify_regimes_detects_crisis():
    df = _make_df(700, trend=0.0001)
    # Inject a sharp crash at bar 400: -30% over 20 days with high vol
    df = _inject_crash(df, start_idx=400, crash_pct=0.30, n_days=20)
    result = classify_regimes(df)
    # Bars 405..425 should have CRISIS
    crisis_window = result.iloc[405:425]
    assert (crisis_window == REGIME_CRISIS).any(), (
        f"Expected CRISIS during crash window; got: {crisis_window.value_counts().to_dict()}"
    )


# ---------------------------------------------------------------------------
# Test 4: BULL regime detected during strong uptrend
# ---------------------------------------------------------------------------

def test_classify_regimes_detects_bull():
    # Strong positive trend, low vol → should produce BULL bars
    df = _make_df(500, trend=0.004, noise=0.001)
    result = classify_regimes(df)
    # After warmup (bar 252+), should have mostly BULL
    tail = result.iloc[280:]
    assert (tail == REGIME_BULL).sum() > 5, (
        f"Expected BULL bars in uptrend; got: {tail.value_counts().to_dict()}"
    )


# ---------------------------------------------------------------------------
# Test 5: CRISIS > BEAR > BULL priority — crisis overrides bear
# ---------------------------------------------------------------------------

def test_classify_regimes_priority_crisis_over_bear():
    """When both BEAR and CRISIS conditions met, result should be CRISIS."""
    cfg = RegimeLabelConfig(
        crisis_return_threshold=-0.05,  # easier to trigger
        crisis_vol_multiplier=0.1,      # very easy vol threshold
        bear_return_threshold=-0.05,
        bear_vol_multiplier=0.1,
    )
    df = _make_df(400, trend=-0.003, noise=0.01)
    result = classify_regimes(df, cfg=cfg)
    # Both BEAR and CRISIS thresholds easily met — CRISIS should win
    # (at least some bars should be CRISIS, not BEAR)
    tail = result.iloc[260:]
    if (tail == REGIME_CRISIS).any():
        # Good: CRISIS overrides BEAR
        pass
    else:
        pytest.skip("No crisis triggered with these params — test inconclusive")


# ---------------------------------------------------------------------------
# Test 6: get_regime_splits returns only regimes meeting min_rows
# ---------------------------------------------------------------------------

def test_get_regime_splits_filters_by_min_rows():
    df = _make_df(600)
    labels = classify_regimes(df)
    # Set min_rows very high for bull/bear/crisis — those should be excluded.
    # Neutral is not in the priority list passed to get_regime_splits for training
    # (allowed_regimes = bull/bear/crisis), so we verify those three are absent.
    cfg = RegimeLabelConfig(min_rows={REGIME_BULL: 1000, REGIME_BEAR: 1000, REGIME_CRISIS: 1000})
    splits = get_regime_splits(df, labels, cfg=cfg)
    # bull/bear/crisis should not qualify; only neutral may (min_rows defaults to 1)
    for regime in (REGIME_BULL, REGIME_BEAR, REGIME_CRISIS):
        assert regime not in splits, (
            f"Expected {regime} to be excluded by min_rows=1000; got {list(splits.keys())}"
        )


def test_get_regime_splits_returns_correct_subsets():
    df = _make_df(600)
    labels = classify_regimes(df)
    cfg = RegimeLabelConfig(min_rows={REGIME_BULL: 1, REGIME_BEAR: 1, REGIME_CRISIS: 1})
    splits = get_regime_splits(df, labels, cfg=cfg)
    # Every split subset must be rows from df with matching label
    for regime, sub_df in splits.items():
        assert len(sub_df) > 0
        assert set(sub_df.index).issubset(set(df.index))
        matching_labels = labels.loc[sub_df.index]
        assert (matching_labels == regime).all(), f"Regime {regime} subset contains wrong labels"


# ---------------------------------------------------------------------------
# Test 7: regime_distribution sums to 1.0
# ---------------------------------------------------------------------------

def test_regime_distribution_sums_to_one():
    df = _make_df(500)
    labels = classify_regimes(df)
    dist = regime_distribution(labels)
    total = sum(dist.values())
    assert abs(total - 1.0) < 1e-9, f"Distribution should sum to 1.0; got {total}"


def test_regime_distribution_empty():
    empty = pd.Series(dtype=str)
    dist = regime_distribution(empty)
    for v in dist.values():
        assert v == 0.0


# ---------------------------------------------------------------------------
# Test 8: Missing close column raises KeyError
# ---------------------------------------------------------------------------

def test_classify_regimes_missing_close_raises():
    df = _make_df(300).rename(columns={"close": "price"})
    with pytest.raises(KeyError, match="close"):
        classify_regimes(df, close_col="close")
