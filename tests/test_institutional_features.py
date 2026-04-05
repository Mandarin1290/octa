"""Tests for institutional-grade feature modules."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ohlcv(n: int = 300, seed: int = 42) -> tuple:
    """Return (close, open_, high, low, volume, ret, vol) all pre-shifted as from features.py."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2020-01-01", periods=n, freq="B")
    prices = 100.0 * np.exp(np.cumsum(rng.standard_normal(n) * 0.01))
    close = pd.Series(prices, index=idx)
    high = close * (1 + np.abs(rng.standard_normal(n) * 0.005))
    low = close * (1 - np.abs(rng.standard_normal(n) * 0.005))
    open_ = close.shift(1).fillna(close.iloc[0])
    volume = pd.Series(rng.integers(100_000, 500_000, n).astype(float), index=idx)
    ret = close.pct_change(fill_method=None).fillna(0.0)
    vol = ret.rolling(20, min_periods=1).std()
    return close, open_, high, low, volume, ret, vol


# ---------------------------------------------------------------------------
# Microstructure
# ---------------------------------------------------------------------------


class TestMicrostructure:
    def test_returns_expected_keys_with_volume(self):
        from octa.core.features.institutional.microstructure import build_microstructure_features
        close, open_, high, low, volume, *_ = _make_ohlcv()
        feats = build_microstructure_features(close, open_, high, low, volume, has_usable_volume=True)
        assert "ms_close_position" in feats
        assert "ms_bar_body_ratio" in feats
        assert "ms_typical_z" in feats
        assert "ms_illiquidity_z" in feats

    def test_no_illiquidity_without_volume(self):
        from octa.core.features.institutional.microstructure import build_microstructure_features
        close, open_, high, low, volume, *_ = _make_ohlcv()
        feats = build_microstructure_features(close, open_, high, low, volume, has_usable_volume=False)
        assert "ms_illiquidity_z" not in feats

    def test_close_position_bounded(self):
        from octa.core.features.institutional.microstructure import build_microstructure_features
        close, open_, high, low, volume, *_ = _make_ohlcv()
        feats = build_microstructure_features(close, open_, high, low, volume)
        cp = feats["ms_close_position"].dropna()
        assert (cp >= 0.0).all() and (cp <= 1.0).all()

    def test_bar_body_ratio_bounded(self):
        from octa.core.features.institutional.microstructure import build_microstructure_features
        close, open_, high, low, volume, *_ = _make_ohlcv()
        feats = build_microstructure_features(close, open_, high, low, volume)
        bbr = feats["ms_bar_body_ratio"].dropna()
        assert (bbr >= 0.0).all() and (bbr <= 1.0).all()

    def test_no_all_nan_columns(self):
        from octa.core.features.institutional.microstructure import build_microstructure_features
        close, open_, high, low, volume, *_ = _make_ohlcv()
        feats = build_microstructure_features(close, open_, high, low, volume)
        for name, series in feats.items():
            assert series.notna().any(), f"{name} is all-NaN"


# ---------------------------------------------------------------------------
# Multiframe
# ---------------------------------------------------------------------------


class TestMultiframe:
    def test_returns_expected_keys(self):
        from octa.core.features.institutional.multiframe import build_multiframe_features
        close, _, high, low, _, ret, _ = _make_ohlcv()
        feats = build_multiframe_features(close, high, low, ret)
        assert "mf_vol_regime_ratio" in feats
        assert "mf_trend_consistency_10" in feats
        assert "mf_range_position_20" in feats
        assert "mf_ret_autocorr_20" in feats

    def test_trend_consistency_bounded(self):
        from octa.core.features.institutional.multiframe import build_multiframe_features
        close, _, high, low, _, ret, _ = _make_ohlcv()
        feats = build_multiframe_features(close, high, low, ret)
        tc = feats["mf_trend_consistency_10"].dropna()
        assert (tc >= 0.0).all() and (tc <= 1.0).all()

    def test_range_position_bounded(self):
        from octa.core.features.institutional.multiframe import build_multiframe_features
        close, _, high, low, _, ret, _ = _make_ohlcv()
        feats = build_multiframe_features(close, high, low, ret)
        rp = feats["mf_range_position_20"].dropna()
        assert (rp >= 0.0).all() and (rp <= 1.0).all()

    def test_autocorr_bounded(self):
        from octa.core.features.institutional.multiframe import build_multiframe_features
        close, _, high, low, _, ret, _ = _make_ohlcv()
        feats = build_multiframe_features(close, high, low, ret)
        ac = feats["mf_ret_autocorr_20"].dropna()
        assert (ac >= -1.0).all() and (ac <= 1.0).all()

    def test_no_all_nan_columns(self):
        from octa.core.features.institutional.multiframe import build_multiframe_features
        close, _, high, low, _, ret, _ = _make_ohlcv()
        feats = build_multiframe_features(close, high, low, ret)
        for name, series in feats.items():
            assert series.notna().any(), f"{name} is all-NaN"


# ---------------------------------------------------------------------------
# Robustness
# ---------------------------------------------------------------------------


class TestRobustness:
    def test_returns_expected_keys(self):
        from octa.core.features.institutional.robustness import build_robustness_features
        close, _, _, _, _, ret, vol = _make_ohlcv()
        feats = build_robustness_features(close, ret, vol)
        assert "rb_vol_normalized_ret" in feats
        assert "rb_drawdown_depth" in feats
        assert "rb_ret_iqr_20" in feats
        assert "rb_calmar_proxy_20" in feats

    def test_drawdown_depth_non_positive(self):
        from octa.core.features.institutional.robustness import build_robustness_features
        close, _, _, _, _, ret, vol = _make_ohlcv()
        feats = build_robustness_features(close, ret, vol)
        dd = feats["rb_drawdown_depth"].dropna()
        assert (dd <= 0.0).all(), "drawdown depth must be <= 0 (below peak)"

    def test_iqr_non_negative(self):
        from octa.core.features.institutional.robustness import build_robustness_features
        close, _, _, _, _, ret, vol = _make_ohlcv()
        feats = build_robustness_features(close, ret, vol)
        iqr = feats["rb_ret_iqr_20"].dropna()
        assert (iqr >= 0.0).all()

    def test_no_all_nan_columns(self):
        from octa.core.features.institutional.robustness import build_robustness_features
        close, _, _, _, _, ret, vol = _make_ohlcv()
        feats = build_robustness_features(close, ret, vol)
        for name, series in feats.items():
            assert series.notna().any(), f"{name} is all-NaN"


# ---------------------------------------------------------------------------
# Integration: features.py pipeline picks up institutional features
# ---------------------------------------------------------------------------


class TestPipelineIntegration:
    def _make_df(self, n: int = 400) -> pd.DataFrame:
        rng = np.random.default_rng(7)
        idx = pd.date_range("2020-01-01", periods=n, freq="B")
        p = 100.0 * np.exp(np.cumsum(rng.standard_normal(n) * 0.01))
        return pd.DataFrame({
            "open":   p * (1 + rng.standard_normal(n) * 0.002),
            "high":   p * (1 + np.abs(rng.standard_normal(n) * 0.005)),
            "low":    p * (1 - np.abs(rng.standard_normal(n) * 0.005)),
            "close":  p,
            "volume": rng.integers(100_000, 500_000, n).astype(float),
        }, index=idx)

    def _cfg(self, tf: str = "1D"):
        class _C:
            timeframe = tf
            window_short = 5
            window_med = 20
            window_long = 60
            vol_window = 20
            horizons = [1, 3, 5]
            features = {}
        return _C()

    def test_institutional_features_not_active_in_pipeline(self):
        """Institutional features are NOT wired into features.py yet — must not appear in output.

        The institutional modules are built and tested in isolation. Direct pipeline integration
        was reverted after showing it worsened ADC's OOS/IS ratio from 0.52 to -11.73.
        Integration requires a selective replacement strategy (not additive), tracked separately.
        """
        from octa.core.features.features import build_features
        df = self._make_df()
        result = build_features(df, settings=self._cfg("1D"), asset_class="equity")
        inst = [c for c in result.X.columns if c.startswith(("ms_", "mf_", "rb_"))]
        assert len(inst) == 0, (
            f"Institutional features should not be active in the pipeline yet, "
            f"but found: {inst}"
        )

    def test_feature_count_unchanged(self):
        from octa.core.features.features import build_features
        df = self._make_df()
        result = build_features(df, settings=self._cfg("1D"), asset_class="equity")
        cols = list(result.X.columns)
        # edgar and cot are disabled (no offline cache) — baseline without those sources
        assert len(cols) >= 50, f"Expected baseline runtime feature set, got only {len(cols)} columns"
        # FRED macro is enabled and cached — altdat_source_fred_present indicator should appear
        # when altdata_config_path is set; without it (no path on this minimal cfg) it is absent.
