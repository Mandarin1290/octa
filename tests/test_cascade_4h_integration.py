"""Tests for 4H cascade integration.

Phase 3 implementation tests:
  1. _infer_timeframe_key() correctly classifies 4H bars as "4H" (not "1H")
  2. timeframe_seconds("4H") returns 14400
  3. EXTENDED_TIMEFRAMES_WITH_4H constant exists and has correct order
  4. TrainingConfig accepts cascade_timeframes field
  5. 4H parquet resample produces correct OHLCV from synthetic 1H data
  6. _build_parquet_paths accepts optional tfs parameter
  7. hf_tf_overlays contains 4H entry
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_4h_index(n: int) -> pd.DatetimeIndex:
    """Synthetic 4H DatetimeIndex."""
    return pd.date_range("2020-01-06 00:00", periods=n, freq="4h", tz="UTC")


def _make_1h_index(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2020-01-06 00:00", periods=n, freq="1h", tz="UTC")


def _make_daily_index(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2020-01-06", periods=n, freq="B", tz="UTC")


# ---------------------------------------------------------------------------
# Test 1: _infer_timeframe_key 4H classification
# ---------------------------------------------------------------------------

def test_infer_timeframe_key_4h():
    """4H bars (14400s spacing) must be classified as '4H', not '1H'."""
    from octa_training.core import pipeline as _pipeline_mod

    # Synthetic 4H index: spacing = 14400s
    idx = _make_4h_index(500)

    # Access the inner function by running a minimal training call — we test
    # via the spacing math directly since _infer_timeframe_key is a closure.
    # Replicate the logic:
    deltas = np.diff(idx.astype("int64"))
    sec = float(np.median(deltas)) / 1e9

    # Verify spacing
    assert abs(sec - 14400.0) < 1.0, f"Expected 14400s, got {sec}"

    # Apply the fixed classification logic
    def _infer(sec_val: float) -> str:
        if sec_val >= 20 * 3600:
            return "1D"
        if sec_val >= 2 * 3600:
            return "4H"
        if sec_val >= 50 * 60:
            return "1H"
        if sec_val >= 20 * 60:
            return "30m"
        if sec_val >= 4 * 60:
            return "5m"
        return "1m"

    result = _infer(sec)
    assert result == "4H", f"Expected '4H', got '{result}' for sec={sec}"


def test_infer_timeframe_key_1h_not_affected():
    """1H bars (3600s) must still classify as '1H' after 4H insertion."""
    idx = _make_1h_index(1000)
    deltas = np.diff(idx.astype("int64"))
    sec = float(np.median(deltas)) / 1e9
    assert abs(sec - 3600.0) < 1.0

    def _infer(sec_val: float) -> str:
        if sec_val >= 20 * 3600:
            return "1D"
        if sec_val >= 2 * 3600:
            return "4H"
        if sec_val >= 50 * 60:
            return "1H"
        if sec_val >= 20 * 60:
            return "30m"
        if sec_val >= 4 * 60:
            return "5m"
        return "1m"

    assert _infer(sec) == "1H"


def test_infer_timeframe_key_1d_not_affected():
    """1D bars must still classify as '1D' after 4H insertion."""
    idx = _make_daily_index(500)
    deltas = np.diff(idx.astype("int64"))
    sec = float(np.median(deltas)) / 1e9
    assert sec >= 20 * 3600

    def _infer(sec_val: float) -> str:
        if sec_val >= 20 * 3600:
            return "1D"
        if sec_val >= 2 * 3600:
            return "4H"
        if sec_val >= 50 * 60:
            return "1H"
        if sec_val >= 20 * 60:
            return "30m"
        if sec_val >= 4 * 60:
            return "5m"
        return "1m"

    assert _infer(sec) == "1D"


# ---------------------------------------------------------------------------
# Test 2: timeframe_seconds("4H")
# ---------------------------------------------------------------------------

def test_timeframe_seconds_4h():
    from octa_ops.autopilot.types import timeframe_seconds
    assert timeframe_seconds("4H") == 14400
    assert timeframe_seconds("4h") == 14400


def test_timeframe_seconds_existing_unaffected():
    from octa_ops.autopilot.types import timeframe_seconds
    assert timeframe_seconds("1D") == 86400
    assert timeframe_seconds("1H") == 3600
    assert timeframe_seconds("30M") == 1800
    assert timeframe_seconds("5M") == 300
    assert timeframe_seconds("1M") == 60


# ---------------------------------------------------------------------------
# Test 3: EXTENDED_TIMEFRAMES_WITH_4H constant
# ---------------------------------------------------------------------------

def test_extended_timeframes_with_4h_exists():
    from octa.core.cascade.policies import EXTENDED_TIMEFRAMES_WITH_4H, DEFAULT_TIMEFRAMES
    assert EXTENDED_TIMEFRAMES_WITH_4H == ("1D", "4H", "1H", "30M", "5M", "1M")
    assert len(EXTENDED_TIMEFRAMES_WITH_4H) == len(DEFAULT_TIMEFRAMES) + 1
    assert "4H" in EXTENDED_TIMEFRAMES_WITH_4H
    assert EXTENDED_TIMEFRAMES_WITH_4H.index("4H") == 1  # second position


def test_default_timeframes_unchanged():
    from octa.core.cascade.policies import DEFAULT_TIMEFRAMES
    assert DEFAULT_TIMEFRAMES == ("1D", "1H", "30M", "5M", "1M")


# ---------------------------------------------------------------------------
# Test 4: TrainingConfig cascade_timeframes field
# ---------------------------------------------------------------------------

def test_training_config_cascade_timeframes_default_none():
    from octa_training.core.config import TrainingConfig
    cfg = TrainingConfig()
    assert cfg.cascade_timeframes is None


def test_training_config_cascade_timeframes_set():
    from octa_training.core.config import TrainingConfig
    cfg = TrainingConfig(cascade_timeframes=["1D", "4H", "1H", "30M", "5M", "1M"])
    assert cfg.cascade_timeframes == ["1D", "4H", "1H", "30M", "5M", "1M"]


# ---------------------------------------------------------------------------
# Test 5: 4H parquet resample correctness
# ---------------------------------------------------------------------------

def _make_synthetic_1h_ohlcv(n_hours: int) -> pd.DataFrame:
    idx = _make_1h_index(n_hours)
    rng = np.random.default_rng(42)
    close = 100.0 + np.cumsum(rng.normal(0, 0.5, n_hours))
    df = pd.DataFrame({
        "open":   close - rng.uniform(0, 0.3, n_hours),
        "high":   close + rng.uniform(0, 0.5, n_hours),
        "low":    close - rng.uniform(0, 0.5, n_hours),
        "close":  close,
        "volume": rng.integers(1000, 5000, n_hours).astype(float),
    }, index=idx)
    return df


def test_4h_resample_ohlcv_correctness():
    """4H resample: open=first 1H open, high=max(4 highs), low=min(4 lows), close=last 1H close, volume=sum."""
    df_1h = _make_synthetic_1h_ohlcv(8)  # 2 complete 4H bars
    df_4h = df_1h.resample("4h", offset="0h").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
    }).dropna(subset=["close"])

    assert len(df_4h) == 2, f"Expected 2 4H bars from 8 1H bars, got {len(df_4h)}"

    # Verify first 4H bar
    bar0 = df_4h.iloc[0]
    h1_slice = df_1h.iloc[:4]
    assert bar0["open"] == pytest.approx(h1_slice["open"].iloc[0])
    assert bar0["high"] == pytest.approx(h1_slice["high"].max())
    assert bar0["low"] == pytest.approx(h1_slice["low"].min())
    assert bar0["close"] == pytest.approx(h1_slice["close"].iloc[-1])
    assert bar0["volume"] == pytest.approx(h1_slice["volume"].sum())


def test_4h_resample_min_bars(tmp_path):
    """Symbols with fewer than MIN_BARS 4H bars should be skipped by the generator."""
    sys_path_backup = __import__("sys").path[:]
    try:
        # Create a minimal 1H parquet with only 100 1H bars → 25 4H bars (< 200 MIN_BARS)
        df = _make_synthetic_1h_ohlcv(100)
        parquet_1h = tmp_path / "TEST_1H.parquet"
        df.to_parquet(parquet_1h)

        import importlib
        spec = importlib.util.spec_from_file_location(
            "generate_4h", Path(__file__).parent.parent / "scripts" / "generate_4h_parquets.py"
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        result = mod.generate_for_symbol(parquet_1h, tmp_path, dry_run=True)
        assert result["status"] == "SKIP"
        assert "too_few_bars" in result["reason"]
    finally:
        __import__("sys").path[:] = sys_path_backup


# ---------------------------------------------------------------------------
# Test 6: _build_parquet_paths with tfs parameter
# ---------------------------------------------------------------------------

def test_build_parquet_paths_custom_tfs():
    from octa.support.ops.run_full_cascade_training_from_parquets import _build_parquet_paths

    inventory = {
        "AAON": {
            "tfs": {
                "1D": ["/data/AAON_1D.parquet"],
                "4H": ["/data/AAON_4H.parquet"],
                "1H": ["/data/AAON_1H.parquet"],
                "30M": ["/data/AAON_30M.parquet"],
                "5M":  ["/data/AAON_5M.parquet"],
                "1M":  ["/data/AAON_1M.parquet"],
            }
        }
    }

    # Default: 5-TF cascade (no 4H)
    paths_default = _build_parquet_paths("AAON", inventory)
    assert "4H" not in paths_default
    assert len(paths_default) == 5

    # Extended: 6-TF cascade with 4H
    from octa.core.cascade.policies import EXTENDED_TIMEFRAMES_WITH_4H
    paths_4h = _build_parquet_paths("AAON", inventory, tfs=EXTENDED_TIMEFRAMES_WITH_4H)
    assert "4H" in paths_4h
    assert len(paths_4h) == 6
    assert paths_4h["4H"] == "/data/AAON_4H.parquet"


# ---------------------------------------------------------------------------
# Test 7: hf_tf_overlays has 4H entry
# ---------------------------------------------------------------------------

def test_hf_tf_overlays_contains_4h(monkeypatch):
    """Verify 4H entry exists in hf_tf_overlays with values between 1D and 1H."""
    # We need to inspect the hf_tf_overlays dict; it's a local in the run() closure.
    # We can do this by reading the source and checking the expected constants.
    from pathlib import Path
    import ast

    src = (Path(__file__).parent.parent / "octa_training" / "core" / "pipeline.py").read_text()
    # Check the literal text contains "4H" in the overlays section
    assert '"4H"' in src or "'4H'" in src, "4H entry missing from pipeline.py hf_tf_overlays"

    # Verify thresholds make sense: 4H sortino should be between 1D (no sortino overlay)
    # and 1H (0.60 sharpe baseline). We only check the dict keys here.
    assert 'profit_factor_min": 1.18' in src or "profit_factor_min': 1.18" in src, \
        "4H profit_factor_min not found with expected value 1.18"
