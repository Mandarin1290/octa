"""Regression tests for leakage_audit altdat_ column exemption.

Root cause (2026-03-01): leakage_audit falsely triggered on altdat_macro_fred_*
features because np.isclose(a, b, rtol=1.0) is asymmetric — it computes
atol + rtol*|b|, so when b (recomputed) is small, the tolerance is tight even
with rtol=1.0. The fix: skip altdat_* columns in the audit loop entirely, since
build_altdata_features enforces temporal integrity independently via
validate_no_future_leakage() and backward asof_join.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from octa.core.features.features import FeatureBuildResult, leakage_audit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _daily_index(n: int = 60) -> pd.DatetimeIndex:
    return pd.date_range("2025-09-01", periods=n, freq="D", tz="UTC")


def _make_raw(index: pd.DatetimeIndex) -> pd.DataFrame:
    n = len(index)
    close = 100.0 + np.arange(n, dtype=float) * 0.1
    return pd.DataFrame(
        {"open": close, "high": close + 0.5, "low": close - 0.5, "close": close, "volume": 1e6},
        index=index,
    )


# ---------------------------------------------------------------------------
# Test 1: altdat_* column differences do NOT trigger leakage_detected
# ---------------------------------------------------------------------------

class TestAltdatExemption:
    """altdat_* feature differences are expected cache variation, not leakage."""

    def test_altdat_diff_does_not_trigger_leakage(self, monkeypatch):
        """leakage_audit must pass even when altdat_* live vs recomputed differ.

        Non-altdat_ columns are constant (50.0 / 0.0) so they match identically
        between full-history X and any truncated recompute. Only altdat_ values
        differ — testing that the audit skips them.
        """
        idx = _daily_index(60)
        horizons = [1]
        n = len(idx)

        # Constant non-altdat values so they match regardless of history length.
        X = pd.DataFrame(
            {
                "rsi_14": np.full(n, 50.0),
                "ret_1": np.zeros(n),
                "altdat_macro_fred_DGS10_chg_1": np.full(n, 0.05),   # live
                "altdat_macro_fred_DGS2_roc_20": np.full(n, 0.10),   # live
            },
            index=idx,
        )
        y_dict = {
            "y_reg_1": pd.Series(np.zeros(n), index=idx),
        }
        raw = _make_raw(idx)

        from octa.core.features import features as feat_module

        def mock_build_features(hist, *, settings=None, asset_class="unknown", build_targets=True):
            k = len(hist)
            # Same non-altdat values as X; VERY different altdat_ values.
            df = pd.DataFrame(
                {
                    "rsi_14": np.full(k, 50.0),
                    "ret_1": np.zeros(k),
                    "altdat_macro_fred_DGS10_chg_1": np.full(k, -0.10),  # opposite sign
                    "altdat_macro_fred_DGS2_roc_20": np.full(k, -0.20),  # opposite sign
                },
                index=hist.index,
            )
            return FeatureBuildResult(X=df, y_dict={}, meta={})

        monkeypatch.setattr(feat_module, "build_features", mock_build_features)

        ok, report = leakage_audit(X, y_dict, raw, horizons, return_report=True)
        assert ok is True, (
            f"leakage_audit must pass for altdat_ cache variation. "
            f"status={report['status']} examples={report['outside_tolerance_examples']}"
        )
        assert report["status"] in {"ok", "audit_drift_ok"}, report["status"]
        for ex in report.get("outside_tolerance_examples", []):
            assert not str(ex.get("feature", "")).startswith("altdat_"), (
                f"altdat_ feature must not appear in outside_tolerance_examples: {ex}"
            )

    def test_altdat_nan_pair_does_not_trigger_leakage(self, monkeypatch):
        """NaN/NaN in altdat_ columns must not count as outside tolerance."""
        idx = _daily_index(30)
        horizons = [1]
        n = len(idx)

        X = pd.DataFrame(
            {
                "rsi_14": np.full(n, 50.0),
                "ret_1": np.zeros(n),
                "altdat_macro_fred_DGS10_chg_1": np.full(n, np.nan),
            },
            index=idx,
        )
        y_dict = {"y_reg_1": pd.Series(np.zeros(n), index=idx)}
        raw = _make_raw(idx)

        from octa.core.features import features as feat_module

        def mock_build_features(hist, *, settings=None, asset_class="unknown", build_targets=True):
            k = len(hist)
            df = pd.DataFrame(
                {
                    "rsi_14": np.full(k, 50.0),
                    "ret_1": np.zeros(k),
                    "altdat_macro_fred_DGS10_chg_1": np.full(k, np.nan),
                },
                index=hist.index,
            )
            return FeatureBuildResult(X=df, y_dict={}, meta={})

        monkeypatch.setattr(feat_module, "build_features", mock_build_features)

        ok, report = leakage_audit(X, y_dict, raw, horizons, return_report=True)
        assert ok is True, report

    def test_altdat_one_nan_one_value_skipped(self, monkeypatch):
        """altdat_ live=NaN, recomputed=value (or vice versa) must not trigger leakage."""
        idx = _daily_index(30)
        horizons = [1]
        n = len(idx)

        X = pd.DataFrame(
            {
                "rsi_14": np.full(n, 50.0),
                "ret_1": np.zeros(n),
                "altdat_macro_fred_DGS10_chg_1": np.full(n, 0.05),  # live has value
            },
            index=idx,
        )
        y_dict = {"y_reg_1": pd.Series(np.zeros(n), index=idx)}
        raw = _make_raw(idx)

        from octa.core.features import features as feat_module

        def mock_build_features(hist, *, settings=None, asset_class="unknown", build_targets=True):
            k = len(hist)
            df = pd.DataFrame(
                {
                    "rsi_14": np.full(k, 50.0),
                    "ret_1": np.zeros(k),
                    "altdat_macro_fred_DGS10_chg_1": np.full(k, np.nan),  # recomputed is NaN
                },
                index=hist.index,
            )
            return FeatureBuildResult(X=df, y_dict={}, meta={})

        monkeypatch.setattr(feat_module, "build_features", mock_build_features)

        ok, report = leakage_audit(X, y_dict, raw, horizons, return_report=True)
        assert ok is True, report


# ---------------------------------------------------------------------------
# Test 2: non-altdat large differences still trigger leakage_detected
# ---------------------------------------------------------------------------

class TestNonAltdatStillDetected:
    """The fix must not accidentally suppress leakage detection for standard features."""

    def test_large_diff_on_standard_feature_fails(self, monkeypatch):
        """leakage_audit must fail when a non-altdat_ feature differs substantially."""
        idx = _daily_index(60)
        horizons = [1]
        n = len(idx)

        X = pd.DataFrame(
            {
                "rsi_14": np.full(n, 50.0),
                "ret_1": np.zeros(n),
            },
            index=idx,
        )
        y_dict = {"y_reg_1": pd.Series(np.zeros(n), index=idx)}
        raw = _make_raw(idx)

        from octa.core.features import features as feat_module

        def mock_build_features(hist, *, settings=None, asset_class="unknown", build_targets=True):
            k = len(hist)
            # rsi_14 wildly different (99.0 vs 50.0) → must trigger leakage_detected
            df = pd.DataFrame(
                {
                    "rsi_14": np.full(k, 99.0),
                    "ret_1": np.zeros(k),
                },
                index=hist.index,
            )
            return FeatureBuildResult(X=df, y_dict={}, meta={})

        monkeypatch.setattr(feat_module, "build_features", mock_build_features)

        ok, report = leakage_audit(X, y_dict, raw, horizons, return_report=True)
        assert ok is False, "leakage_audit must fail when non-altdat feature differs"
        assert report["status"] == "leakage_detected"

    def test_identical_values_pass(self, monkeypatch):
        """Sanity: identical live and recomputed non-altdat values must pass."""
        idx = _daily_index(30)
        horizons = [1]
        n = len(idx)

        X = pd.DataFrame(
            {
                "rsi_14": np.full(n, 50.0),
                "ret_1": np.zeros(n),
                "altdat_macro_fred_DGS10_chg_1": np.full(n, 0.05),
            },
            index=idx,
        )
        y_dict = {"y_reg_1": pd.Series(np.zeros(n), index=idx)}
        raw = _make_raw(idx)

        from octa.core.features import features as feat_module

        def mock_build_features(hist, *, settings=None, asset_class="unknown", build_targets=True):
            k = len(hist)
            # All values match X exactly — altdat_ varies (should be skipped)
            df = pd.DataFrame(
                {
                    "rsi_14": np.full(k, 50.0),
                    "ret_1": np.zeros(k),
                    "altdat_macro_fred_DGS10_chg_1": np.full(k, -99.9),  # skipped
                },
                index=hist.index,
            )
            return FeatureBuildResult(X=df, y_dict={}, meta={})

        monkeypatch.setattr(feat_module, "build_features", mock_build_features)

        ok, report = leakage_audit(X, y_dict, raw, horizons, return_report=True)
        assert ok is True, report
