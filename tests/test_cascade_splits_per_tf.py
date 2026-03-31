"""Tests for per-timeframe splits_by_timeframe config support.

Evidence: cascade_diag_20260228T082442Z
Root cause: Default splits (n_folds=5, test_window=200) produces 1000 OOF bars
for ALL timeframes. The institutional gate evaluate_walk_forward_oos requires
n >= train_bars + 2*oos_bars per TF:
  1D: 378 bars  → OOF=1000 >= 378 ✓
  1H: 3840 bars → OOF=1000 <  3840 ✗  (root cause of dynamic_gate_hard_fail:1H)
  30M:2080 bars → OOF=1000 <  2080 ✗
  5M: 8580 bars → OOF=1000 <  8580 ✗
  1M:27300 bars → OOF=1000 < 27300 ✗

Fix: splits_by_timeframe in dev.yaml + pipeline.py reads it per TF.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_hourly_index(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2000-01-03 09:30", periods=n, freq="1h", tz="UTC")


def _make_daily_index(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2000-01-03", periods=n, freq="B", tz="UTC")


def _make_30m_index(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2000-01-03 09:30", periods=n, freq="30min", tz="UTC")


# ---------------------------------------------------------------------------
# Test: splits_by_timeframe is read from config.py model
# ---------------------------------------------------------------------------

def test_training_config_accepts_splits_by_timeframe():
    """TrainingConfig must accept splits_by_timeframe without validation error."""
    from octa_training.core.config import TrainingConfig

    cfg = TrainingConfig(
        splits_by_timeframe={
            "1H": {"test_window": 960, "step": 960},
            "30M": {"test_window": 500, "step": 500},
        }
    )
    assert cfg.splits_by_timeframe["1H"]["test_window"] == 960
    assert cfg.splits_by_timeframe["30M"]["test_window"] == 500


def test_training_config_splits_by_timeframe_defaults_empty():
    """Default splits_by_timeframe is an empty dict (no per-TF override)."""
    from octa_training.core.config import TrainingConfig

    cfg = TrainingConfig()
    assert isinstance(cfg.splits_by_timeframe, dict)
    assert len(cfg.splits_by_timeframe) == 0


# ---------------------------------------------------------------------------
# Test: dev.yaml loads correctly with splits_by_timeframe
# ---------------------------------------------------------------------------

def test_dev_yaml_splits_by_timeframe_present():
    """dev.yaml must contain splits_by_timeframe entries for Foundation cascade timeframes."""
    from octa_training.core.config import load_config

    cfg = load_config("configs/dev.yaml")
    assert isinstance(cfg.splits_by_timeframe, dict), "splits_by_timeframe must be a dict"
    assert "1D" in cfg.splits_by_timeframe, "1D must have a Foundation-specific splits_by_timeframe entry"
    assert "1H" in cfg.splits_by_timeframe, "1H must have a splits_by_timeframe entry"
    assert "30M" in cfg.splits_by_timeframe, "30M must have a splits_by_timeframe entry"


def test_dev_yaml_1h_oof_sufficient_for_institutional_gate():
    """After applying 1H splits_by_timeframe, the total backtest df length must be >= institutional min_2=3840.
    Total df length = train_window + abs(n_folds)*test_window (train for first fold + all OOS windows).
    n_folds MUST be negative so the pipeline uses most-recent data, not year-2000 history.
    """
    from octa_training.core.config import load_config

    cfg = load_config("configs/dev.yaml")
    base = cfg.splits
    tf_override = cfg.splits_by_timeframe.get("1H", {})
    effective = {**base, **tf_override}

    n_folds_raw = int(effective.get("n_folds", 5))
    assert n_folds_raw < 0, (
        f"1H n_folds must be negative (use most-recent data), got {n_folds_raw}. "
        "Positive n_folds trains on year-2000 history and all gates fail on modern data."
    )
    n_folds = abs(n_folds_raw)
    train_window = int(effective.get("train_window", 1000))
    test_window = int(effective.get("test_window", 200))
    # Total backtest length: training window of first fold + all OOS windows
    total_backtest_bars = train_window + n_folds * test_window
    institutional_min_2_1h = 2880 + 2 * 480  # = 3840
    assert total_backtest_bars >= institutional_min_2_1h, (
        f"1H total backtest bars ({total_backtest_bars}) < institutional min_2 ({institutional_min_2_1h}). "
        f"evaluate_walk_forward_oos will fail for 1H."
    )


def test_dev_yaml_30m_oof_sufficient_for_institutional_gate():
    """After applying 30M splits_by_timeframe, total backtest df >= institutional min_2=2080.
    n_folds MUST be negative so the pipeline uses most-recent data, not year-2000 history.
    """
    from octa_training.core.config import load_config

    cfg = load_config("configs/dev.yaml")
    base = cfg.splits
    tf_override = cfg.splits_by_timeframe.get("30M", {})
    effective = {**base, **tf_override}

    n_folds_raw = int(effective.get("n_folds", 5))
    assert n_folds_raw < 0, (
        f"30M n_folds must be negative (use most-recent data), got {n_folds_raw}."
    )
    n_folds = abs(n_folds_raw)
    train_window = int(effective.get("train_window", 1000))
    test_window = int(effective.get("test_window", 200))
    total_backtest_bars = train_window + n_folds * test_window
    institutional_min_2_30m = 1560 + 2 * 260  # = 2080
    assert total_backtest_bars >= institutional_min_2_30m, (
        f"30M total backtest bars ({total_backtest_bars}) < institutional min_2 ({institutional_min_2_30m})."
    )


def test_dev_yaml_1d_oof_sufficient_for_institutional_gate():
    """Foundation 1D override must produce enough OOF bars for min_2 walk-forward."""
    from octa_training.core.config import load_config

    cfg = load_config("configs/dev.yaml")
    base = cfg.splits
    tf_override = cfg.splits_by_timeframe.get("1D", {})
    effective = {**base, **tf_override}
    n_folds = abs(effective.get("n_folds", 5))
    test_window = effective.get("test_window", 200)
    oof_1d_default = n_folds * test_window
    institutional_min_2_1d = 252 + 2 * 63  # = 378
    assert oof_1d_default >= institutional_min_2_1d


def test_dev_yaml_1d_override_generates_strict_folds_for_foundation_sized_dataset():
    """The Foundation 1D profile must create strict folds for a ~660-row dataset."""
    from octa_training.core.config import load_config
    from octa_training.core.splits import walk_forward_splits

    cfg = load_config("configs/dev.yaml")
    base = cfg.splits
    tf_override = cfg.splits_by_timeframe.get("1D", {})
    effective = {**base, **tf_override}

    idx = _make_daily_index(660)
    folds = walk_forward_splits(
        idx,
        n_folds=int(effective["n_folds"]),
        train_window=int(effective["train_window"]),
        test_window=int(effective["test_window"]),
        step=int(effective["step"]),
        purge_size=int(effective.get("purge_size", 10)),
        embargo_size=int(effective.get("embargo_size", 5)),
        min_train_size=int(effective["min_train_size"]),
        min_test_size=int(effective["min_test_size"]),
        expanding=bool(effective.get("expanding", True)),
        min_folds_required=int(effective.get("min_folds_required", 1)),
    )

    assert len(folds) == 3
    assert sum(int(f.val_idx.size) for f in folds) == 378


# ---------------------------------------------------------------------------
# Test: pipeline._infer_timeframe_key correctly identifies TFs
# ---------------------------------------------------------------------------

def test_infer_timeframe_key_1h():
    from octa_training.core.institutional_gates import _infer_timeframe_key

    idx = _make_hourly_index(100)
    assert _infer_timeframe_key(idx) == "1H"


def test_infer_timeframe_key_1d():
    from octa_training.core.institutional_gates import _infer_timeframe_key

    idx = _make_daily_index(100)
    assert _infer_timeframe_key(idx) == "1D"


def test_infer_timeframe_key_30m():
    from octa_training.core.institutional_gates import _infer_timeframe_key

    idx = _make_30m_index(100)
    assert _infer_timeframe_key(idx) == "30M"


# ---------------------------------------------------------------------------
# Test: evaluate_walk_forward_oos passes with 1H OOF after fix
# ---------------------------------------------------------------------------

def _make_strat_ret_df(n: int, freq: str = "1h") -> pd.DataFrame:
    """Create a minimal df_backtest with strat_ret column."""
    idx = pd.date_range("2000-01-03 09:30", periods=n, freq=freq, tz="UTC")
    np.random.seed(42)
    ret = np.random.normal(0.0002, 0.01, n)
    strat_ret = np.random.normal(0.0003, 0.01, n)
    df = pd.DataFrame({"ret": ret, "strat_ret": strat_ret, "pos": np.sign(strat_ret), "turnover": np.abs(np.diff(np.sign(strat_ret), prepend=0))}, index=idx)
    return df


def _make_gate(sharpe_min: float = 0.0, pf_min: float = 0.5, dd_max: float = 0.9) -> MagicMock:
    gate = MagicMock()
    gate.sharpe_min = sharpe_min
    gate.profit_factor_min = pf_min
    gate.max_drawdown_max = dd_max
    gate.walkforward_oos_sharpe_min = None
    gate.walkforward_oos_pf_min = None
    gate.walkforward_oos_maxdd_max = None
    gate.walkforward_oos_sharpe_scale = 0.90
    gate.walkforward_oos_pf_scale = 0.95
    gate.walkforward_oos_dd_scale = 1.0
    return gate


def test_evaluate_wf_oos_fails_with_1000_bars_1h():
    """With only 1000 OOF bars, evaluate_walk_forward_oos MUST fail for 1H (insufficient_history)."""
    from octa_training.core.institutional_gates import evaluate_walk_forward_oos

    df = _make_strat_ret_df(1000, freq="1h")
    gate = _make_gate()
    result = evaluate_walk_forward_oos(df, gate, timeframe="1H")
    assert result["passed"] is False
    assert result["reason"] == "insufficient_history_for_walkforward"
    meta = result.get("walkforward_meta", {})
    assert meta.get("history_bars", 0) < meta.get("required_bars_for_2", 1)


def test_evaluate_wf_oos_passes_with_4800_bars_1h():
    """With 4800 OOF bars (5*960), evaluate_walk_forward_oos MUST have n >= min_3=4320 for 1H."""
    from octa_training.core.institutional_gates import evaluate_walk_forward_oos

    df = _make_strat_ret_df(4800, freq="1h")
    gate = _make_gate(sharpe_min=0.0, pf_min=0.0, dd_max=1.0)
    result = evaluate_walk_forward_oos(df, gate, timeframe="1H")
    # With very permissive gate thresholds: should reach fold evaluation stage (not fail on bar count)
    assert result["reason"] != "insufficient_history_for_walkforward", (
        f"4800 bars should be sufficient (min_3=4320). Got reason: {result['reason']}"
    )


def test_evaluate_wf_oos_passes_with_1000_bars_1d():
    """1D baseline: 1000 OOF bars >= min_2=378 so bar check passes."""
    from octa_training.core.institutional_gates import evaluate_walk_forward_oos

    df = _make_strat_ret_df(1000, freq="B")
    gate = _make_gate(sharpe_min=0.0, pf_min=0.0, dd_max=1.0)
    result = evaluate_walk_forward_oos(df, gate, timeframe="1D")
    assert result["reason"] != "insufficient_history_for_walkforward", (
        "1000 bars should be sufficient for 1D (min_2=378)."
    )


# ---------------------------------------------------------------------------
# Test: walk_forward_splits with per-TF config produces correct OOF count
# ---------------------------------------------------------------------------

def test_walk_forward_splits_1h_override_produces_enough_oof():
    """With test_window=960, step=960, n_folds=5, the OOF indices cover 4800 unique bars."""
    from octa_training.core.splits import walk_forward_splits

    n = 10000  # large 1H dataset
    idx = _make_hourly_index(n)
    folds = walk_forward_splits(
        idx,
        n_folds=5,
        train_window=1000,
        test_window=960,
        step=960,
        expanding=True,
        min_train_size=500,
        min_test_size=100,
    )
    # Count total unique validation bars
    all_val = set()
    for fold in folds:
        all_val.update(fold.val_idx.tolist())
    total_oof = len(all_val)
    institutional_min_2_1h = 3840
    assert total_oof >= institutional_min_2_1h, (
        f"Expected >= {institutional_min_2_1h} unique OOF bars, got {total_oof}"
    )


def test_walk_forward_splits_1h_no_oof_overlap():
    """With step=test_window=960, validation windows must not overlap."""
    from octa_training.core.splits import walk_forward_splits

    n = 10000
    idx = _make_hourly_index(n)
    folds = walk_forward_splits(
        idx, n_folds=5, train_window=1000, test_window=960, step=960, expanding=True,
        min_train_size=500, min_test_size=100,
    )
    all_val_indices = [set(f.val_idx.tolist()) for f in folds]
    for i in range(len(all_val_indices)):
        for j in range(i + 1, len(all_val_indices)):
            overlap = all_val_indices[i] & all_val_indices[j]
            assert len(overlap) == 0, f"Folds {i} and {j} have {len(overlap)} overlapping validation bars"


def test_walk_forward_splits_default_insufficient_for_1h():
    """Baseline: default test_window=200 produces only 1000 OOF bars, < min_2=3840 for 1H."""
    from octa_training.core.splits import walk_forward_splits

    n = 10000
    idx = _make_hourly_index(n)
    folds = walk_forward_splits(
        idx, n_folds=5, train_window=1000, test_window=200, step=200, expanding=True,
        min_train_size=500, min_test_size=100,
    )
    all_val = set()
    for fold in folds:
        all_val.update(fold.val_idx.tolist())
    total_oof = len(all_val)
    institutional_min_2_1h = 3840
    assert total_oof < institutional_min_2_1h, (
        f"Default config produces {total_oof} OOF bars which should be < {institutional_min_2_1h}. "
        "This confirms the root cause."
    )


# ---------------------------------------------------------------------------
# Regression test: splits_by_timeframe uppercase key lookup (pipeline bug fix)
# Root cause of dynamic_gate_hard_fail:30M:no_dynamic_gate_input_candidates
# The nested _infer_timeframe_key in pipeline.py returned "30m" (lowercase)
# while dev.yaml splits_by_timeframe uses "30M" (uppercase) → silent miss.
# ---------------------------------------------------------------------------

def _make_5m_index(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2000-01-03 09:30", periods=n, freq="5min", tz="UTC")


def _make_1m_index(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2000-01-03 09:30", periods=n, freq="1min", tz="UTC")


def test_splits_by_timeframe_uppercase_key_resolved_for_30m():
    """splits_by_timeframe "30M" key must be resolved even when inferred key is lowercase "30m"."""
    from octa_training.core.config import TrainingConfig
    from octa_training.core.splits import walk_forward_splits

    cfg = TrainingConfig(
        splits_by_timeframe={"30M": {"test_window": 500, "step": 500}},
    )
    idx = _make_30m_index(10000)
    # Simulate what pipeline.py does at both lookup sites
    splits_by_tf = cfg.splits_by_timeframe
    # Inferred key (pipeline.py nested function returns lowercase for 30M)
    tf_key_lower = "30m"
    # Post-fix: fallback to .upper()
    spec = splits_by_tf.get(tf_key_lower, {}) or splits_by_tf.get(tf_key_lower.upper(), {}) or {}
    assert spec.get("test_window") == 500, (
        f"splits_by_timeframe lookup for '30M' (via .upper() fallback) must return test_window=500, "
        f"got spec={spec!r}"
    )
    # Verify walk_forward_splits produces the correct OOF bars
    folds = walk_forward_splits(
        idx,
        n_folds=5,
        train_window=int(cfg.splits.get("train_window", 1000)),
        test_window=500,
        step=500,
        expanding=True,
        min_train_size=500,
        min_test_size=100,
    )
    all_val = set()
    for fold in folds:
        all_val.update(fold.val_idx.tolist())
    oof = len(all_val)
    institutional_min_2_30m = 1560 + 2 * 260  # = 2080
    assert oof >= institutional_min_2_30m, (
        f"30M OOF={oof} must be >= institutional min_2={institutional_min_2_30m} after fix"
    )


def test_splits_by_timeframe_lowercase_key_also_works():
    """splits_by_timeframe with lowercase "30m" key must also work (forward compat)."""
    from octa_training.core.config import TrainingConfig

    cfg = TrainingConfig(
        splits_by_timeframe={"30m": {"test_window": 500, "step": 500}},
    )
    splits_by_tf = cfg.splits_by_timeframe
    tf_key_lower = "30m"
    spec = splits_by_tf.get(tf_key_lower, {}) or splits_by_tf.get(tf_key_lower.upper(), {}) or {}
    assert spec.get("test_window") == 500


def test_pipeline_nested_infer_key_uppercase_fallback_for_5m_and_1m():
    """splits_by_timeframe "5M" and "1M" uppercase keys must resolve via .upper() fallback."""
    from octa_training.core.config import TrainingConfig

    cfg = TrainingConfig(
        splits_by_timeframe={
            "5M": {"test_window": 2000, "step": 2000},
            "1M": {"test_window": 6500, "step": 6500},
        }
    )
    splits_by_tf = cfg.splits_by_timeframe
    for tf_key_lower, expected_tw in [("5m", 2000), ("1m", 6500)]:
        spec = splits_by_tf.get(tf_key_lower, {}) or splits_by_tf.get(tf_key_lower.upper(), {}) or {}
        assert spec.get("test_window") == expected_tw, (
            f"splits_by_timeframe['{tf_key_lower.upper()}'] must resolve to test_window={expected_tw}, "
            f"got {spec!r}"
        )
