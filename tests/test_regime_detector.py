"""Tests for octa_training.core.regime_detector (v0.1.0)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from octa_training.core.regime_labels import REGIME_NEUTRAL
from octa_training.core.regime_detector import RegimeDetector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(n: int, trend: float = 0.0001) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    close = 100.0 * np.cumprod(1 + trend + rng.normal(0, 0.005, n))
    idx = pd.date_range("2012-01-01", periods=n, freq="B")
    return pd.DataFrame({"close": close}, index=idx)


# ---------------------------------------------------------------------------
# Test 1: fit() raises ValueError on insufficient bars
# ---------------------------------------------------------------------------

def test_fit_raises_on_insufficient_bars():
    det = RegimeDetector()
    df = _make_df(100)
    with pytest.raises(ValueError, match="252"):
        det.fit(df)


# ---------------------------------------------------------------------------
# Test 2: fit() raises ValueError on missing close column
# ---------------------------------------------------------------------------

def test_fit_raises_on_missing_close():
    det = RegimeDetector()
    df = _make_df(300).rename(columns={"close": "price"})
    with pytest.raises(ValueError, match="close"):
        det.fit(df)


# ---------------------------------------------------------------------------
# Test 3: predict() raises RuntimeError before fit
# ---------------------------------------------------------------------------

def test_predict_raises_before_fit():
    det = RegimeDetector()
    df = _make_df(300)
    with pytest.raises(RuntimeError, match="fit"):
        det.predict(df)


# ---------------------------------------------------------------------------
# Test 4: fit + predict returns aligned Series
# ---------------------------------------------------------------------------

def test_fit_predict_aligned():
    det = RegimeDetector()
    df = _make_df(500)
    det.fit(df)
    labels = det.predict(df)
    assert len(labels) == len(df)
    assert (labels.index == df.index).all()


# ---------------------------------------------------------------------------
# Test 5: predict returns NEUTRAL-only on short df (graceful degradation)
# ---------------------------------------------------------------------------

def test_predict_graceful_on_short_df():
    det = RegimeDetector()
    train_df = _make_df(500)
    det.fit(train_df)
    short_df = _make_df(50)
    labels = det.predict(short_df)
    assert (labels == REGIME_NEUTRAL).all()


# ---------------------------------------------------------------------------
# Test 6: save/load round-trip preserves _fitted flag and config
# ---------------------------------------------------------------------------

def test_save_load_roundtrip(tmp_path):
    det = RegimeDetector()
    df = _make_df(500)
    det.fit(df)

    pkl_path = tmp_path / "AAPL_1D_regime.pkl"
    det.save(pkl_path)
    assert pkl_path.exists()

    det2 = RegimeDetector.load(pkl_path)
    assert det2._fitted is True
    assert det2.cfg.rolling_return_window == det.cfg.rolling_return_window

    # predict works after load
    labels = det2.predict(df)
    assert len(labels) == len(df)


# ---------------------------------------------------------------------------
# Test 7: load raises FileNotFoundError on missing path
# ---------------------------------------------------------------------------

def test_load_raises_on_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        RegimeDetector.load(tmp_path / "nonexistent.pkl")


# ---------------------------------------------------------------------------
# Test 8: current_regime returns a valid regime string
# ---------------------------------------------------------------------------

def test_current_regime_returns_valid_string():
    from octa_training.core.regime_labels import _REGIME_PRIORITY
    det = RegimeDetector()
    df = _make_df(500)
    det.fit(df)
    regime = det.current_regime(df)
    assert regime in _REGIME_PRIORITY
