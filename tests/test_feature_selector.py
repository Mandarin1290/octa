"""Tests for octa.core.features.selector — feature deduplication & selection."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from octa.core.features.selector import select_features


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_X(n: int = 200, n_feats: int = 10, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2020-01-01", periods=n, freq="B")
    data = {f"f{i}": rng.standard_normal(n) for i in range(n_feats)}
    return pd.DataFrame(data, index=idx)


def _make_y(n: int = 200, seed: int = 1) -> pd.Series:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2020-01-01", periods=n, freq="B")
    return pd.Series((rng.standard_normal(n) > 0).astype(float), index=idx, name="y_cls_1")


# ---------------------------------------------------------------------------
# Basic correctness
# ---------------------------------------------------------------------------


class TestSelectFeaturesBasic:
    def test_returns_list_of_strings(self):
        X = _make_X()
        result = select_features(X)
        assert isinstance(result, list)
        assert all(isinstance(c, str) for c in result)

    def test_all_returned_cols_exist_in_X(self):
        X = _make_X()
        result = select_features(X)
        assert set(result).issubset(set(X.columns))

    def test_empty_df_returns_empty(self):
        X = pd.DataFrame()
        assert select_features(X) == []

    def test_no_reduction_needed(self):
        """With threshold=1.0 and max_features=100, should return all columns."""
        X = _make_X(n_feats=10)
        result = select_features(X, corr_threshold=1.0, max_features=100)
        assert set(result) == set(X.columns)

    def test_deterministic(self):
        X = _make_X(n_feats=20, seed=42)
        r1 = select_features(X, corr_threshold=0.9, max_features=15)
        r2 = select_features(X, corr_threshold=0.9, max_features=15)
        assert r1 == r2


# ---------------------------------------------------------------------------
# Corr deduplication
# ---------------------------------------------------------------------------


class TestCorrDeduplication:
    def test_drops_near_perfect_duplicate(self):
        """A column that is 0.99-corr with an earlier column must be dropped."""
        rng = np.random.default_rng(7)
        n = 200
        idx = pd.date_range("2020-01-01", periods=n, freq="B")
        base = rng.standard_normal(n)
        noise = rng.standard_normal(n) * 0.01
        X = pd.DataFrame({"a": base, "b": base + noise, "c": rng.standard_normal(n)}, index=idx)
        result = select_features(X, corr_threshold=0.95, max_features=10)
        # 'b' should be dropped because |corr(a, b)| ≈ 0.9999 > 0.95
        assert "a" in result
        assert "b" not in result
        assert "c" in result

    def test_keeps_all_when_threshold_1(self):
        rng = np.random.default_rng(8)
        n = 200
        idx = pd.date_range("2020-01-01", periods=n, freq="B")
        base = rng.standard_normal(n)
        noise = rng.standard_normal(n) * 0.01
        X = pd.DataFrame({"a": base, "b": base + noise, "c": rng.standard_normal(n)}, index=idx)
        result = select_features(X, corr_threshold=1.0, max_features=10)
        assert set(result) == {"a", "b", "c"}

    def test_order_preserved(self):
        """Selected features should maintain original column order."""
        rng = np.random.default_rng(9)
        n = 200
        idx = pd.date_range("2020-01-01", periods=n, freq="B")
        cols = {f"f{i:02d}": rng.standard_normal(n) for i in range(15)}
        X = pd.DataFrame(cols, index=idx)
        result = select_features(X, corr_threshold=0.95, max_features=15)
        original_order = [c for c in X.columns if c in result]
        assert result == original_order


# ---------------------------------------------------------------------------
# max_features cap
# ---------------------------------------------------------------------------


class TestMaxFeaturesCap:
    def test_result_never_exceeds_max_features(self):
        X = _make_X(n_feats=20)
        for max_f in [5, 10, 15]:
            result = select_features(X, corr_threshold=1.0, max_features=max_f)
            assert len(result) <= max_f, f"max_features={max_f} violated"

    def test_cap_with_target_ranking(self):
        """When y is provided and remaining > max_features, use target corr ranking."""
        X = _make_X(n_feats=20)
        y = _make_y()
        result = select_features(X, y=y, corr_threshold=1.0, max_features=8)
        assert len(result) <= 8
        assert all(c in X.columns for c in result)

    def test_cap_without_target_is_deterministic(self):
        """Without y, cap should just return the first max_features in column order."""
        X = _make_X(n_feats=20)
        result = select_features(X, y=None, corr_threshold=1.0, max_features=10)
        assert result == list(X.columns[:10])


# ---------------------------------------------------------------------------
# Robustness / edge cases
# ---------------------------------------------------------------------------


class TestRobustness:
    def test_handles_nan_in_X(self):
        rng = np.random.default_rng(5)
        n = 100
        idx = pd.date_range("2020-01-01", periods=n, freq="B")
        data = {f"f{i}": rng.standard_normal(n) for i in range(6)}
        X = pd.DataFrame(data, index=idx)
        X.iloc[:10, 0] = np.nan  # introduce NaN
        result = select_features(X, corr_threshold=0.9)
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_handles_constant_column(self):
        """A constant column has 0 variance — corr is NaN; must not crash."""
        rng = np.random.default_rng(6)
        n = 100
        idx = pd.date_range("2020-01-01", periods=n, freq="B")
        X = pd.DataFrame(
            {"const": np.ones(n), "var": rng.standard_normal(n)},
            index=idx,
        )
        result = select_features(X, corr_threshold=0.95)
        assert isinstance(result, list)

    def test_single_column(self):
        X = pd.DataFrame({"f0": np.arange(50.0)})
        result = select_features(X, corr_threshold=0.95, max_features=10)
        assert result == ["f0"]
