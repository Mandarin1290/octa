"""Tests für Value Features (build_value_features + build_features integration)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def _make_idx(start="2015-01-01", end="2025-12-31") -> pd.DatetimeIndex:
    return pd.date_range(start, end, freq="B")


def _make_price(idx: pd.DatetimeIndex, seed: int = 42) -> pd.Series:
    rng = np.random.default_rng(seed)
    prices = 100 + np.cumsum(rng.standard_normal(len(idx)) * 0.5)
    return pd.Series(prices, index=idx, name="close")


# ---------------------------------------------------------------------------
# Unit tests for build_value_features
# ---------------------------------------------------------------------------


class TestBuildValueFeatures:
    def test_returns_df_or_none(self):
        from octa.core.features.features import build_value_features
        idx = _make_idx()
        result = build_value_features(idx, symbol="AAPL")
        assert result is None or isinstance(result, pd.DataFrame)

    def test_missing_db_returns_none(self, tmp_path):
        """Non-existent altdat path → None, no exception."""
        from octa.core.features.features import build_value_features
        # Temporarily monkey-patch the path via a symbol that cannot exist
        idx = _make_idx()
        # With no edgar_fundamentals data, returns None
        result = build_value_features(idx, symbol="__NOSYM__")
        assert result is None

    def test_no_lookahead(self):
        """Value at truncation date must match full-history value."""
        from octa.core.features.features import build_value_features

        idx_full  = _make_idx("2015-01-01", "2025-12-31")
        idx_trunc = _make_idx("2015-01-01", "2022-12-31")
        check_date = pd.Timestamp("2022-12-30")

        p_full  = _make_price(idx_full)
        p_trunc = _make_price(idx_trunc)

        r_full  = build_value_features(idx_full,  "AAPL", price_series=p_full)
        r_trunc = build_value_features(idx_trunc, "AAPL", price_series=p_trunc)

        if r_full is None or r_trunc is None:
            pytest.skip("EDGAR data not available in test environment")

        for col in r_full.columns:
            if col not in r_trunc.columns:
                continue
            if check_date not in r_full.index or check_date not in r_trunc.index:
                continue
            v_full  = float(r_full.loc[check_date, col])
            v_trunc = float(r_trunc.loc[check_date, col])
            assert abs(v_full - v_trunc) < 0.01, (
                f"Potential lookahead in {col}: full={v_full:.4f} trunc={v_trunc:.4f}"
            )

    def test_normalized_0_to_1(self):
        """All feature values must be in [0, 1]."""
        from octa.core.features.features import build_value_features

        idx = _make_idx()
        price = _make_price(idx)
        result = build_value_features(idx, "AAPL", price_series=price)
        if result is None:
            pytest.skip("EDGAR data not available")

        for col in result.columns:
            lo = result[col].min()
            hi = result[col].max()
            assert lo >= -0.01, f"{col} has values below 0: min={lo}"
            assert hi <= 1.01, f"{col} has values above 1: max={hi}"

    def test_no_nan_after_fillna(self):
        """No NaN should remain after ffill + fillna(0.5)."""
        from octa.core.features.features import build_value_features

        idx = _make_idx()
        price = _make_price(idx)
        result = build_value_features(idx, "AAPL", price_series=price)
        if result is None:
            pytest.skip("EDGAR data not available")

        assert not result.isna().any().any(), "NaN found in value features"

    def test_composite_present_when_data_available(self):
        from octa.core.features.features import build_value_features

        idx = _make_idx()
        price = _make_price(idx)
        result = build_value_features(idx, "AAPL", price_series=price)
        if result is None:
            pytest.skip("EDGAR data not available")

        assert "value_composite" in result.columns

    def test_value_composite_between_0_1(self):
        from octa.core.features.features import build_value_features

        idx = _make_idx()
        price = _make_price(idx)
        result = build_value_features(idx, "AAPL", price_series=price)
        if result is None or "value_composite" not in result.columns:
            pytest.skip("EDGAR data not available")

        assert result["value_composite"].between(0.0, 1.0).all()


# ---------------------------------------------------------------------------
# Integration: build_features picks up value features when symbol is passed
# ---------------------------------------------------------------------------


class TestBuildFeaturesIntegration:
    def _make_df(self, n: int = 400) -> pd.DataFrame:
        rng = np.random.default_rng(7)
        idx = pd.date_range("2015-01-01", periods=n, freq="B")
        p = 100.0 * np.exp(np.cumsum(rng.standard_normal(n) * 0.01))
        return pd.DataFrame({
            "open":   p,
            "high":   p * 1.01,
            "low":    p * 0.99,
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

    def test_symbol_parameter_accepted(self):
        """build_features() must accept symbol kwarg without error."""
        from octa.core.features.features import build_features
        df = self._make_df()
        # Should not raise even if no EDGAR data
        result = build_features(df, settings=self._cfg("1D"),
                                asset_class="equity", symbol="AAPL")
        assert result is not None
        assert result.X.shape[1] >= 50

    def test_no_symbol_still_works(self):
        """Old call-sites without symbol must still work."""
        from octa.core.features.features import build_features
        df = self._make_df()
        result = build_features(df, settings=self._cfg("1D"), asset_class="equity")
        assert result is not None
        assert result.X.shape[1] >= 50

    def test_no_value_features_without_edgar_data(self):
        """Without edgar_fundamentals data, no value_ columns should appear."""
        from octa.core.features.features import build_features
        df = self._make_df()
        result = build_features(df, settings=self._cfg("1D"),
                                asset_class="equity", symbol="__NOSYM__")
        val_cols = [c for c in result.X.columns if c.startswith("value_")]
        assert len(val_cols) == 0, f"Unexpected value cols with no EDGAR: {val_cols}"

    def test_intraday_does_not_get_value_features(self):
        """Value features are 1D-only; should not appear for 1H timeframe."""
        from octa.core.features.features import build_features
        rng = np.random.default_rng(9)
        n = 500
        idx = pd.date_range("2024-01-01", periods=n, freq="h")
        p = 100.0 * np.exp(np.cumsum(rng.standard_normal(n) * 0.001))
        df = pd.DataFrame({
            "open": p, "high": p * 1.001, "low": p * 0.999,
            "close": p, "volume": rng.integers(1000, 5000, n).astype(float),
        }, index=idx)
        result = build_features(df, settings=self._cfg("1H"),
                                asset_class="equity", symbol="AAPL")
        val_cols = [c for c in result.X.columns if c.startswith("value_")]
        assert len(val_cols) == 0, f"value_ cols should not appear for 1H: {val_cols}"
