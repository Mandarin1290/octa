"""A4 regression: zero-weight altdata sources are excluded from feature output.

Root cause: feature_builder.py computed weights but never applied them —
feat_parts was always concatenated unconditionally.
Fix: feat_parts tagged (source, df); active_parts = [df for src, df in feat_parts if w1.get(src, 1.0) != 0.0]
"""
from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest


def _make_bars(n: int = 10) -> pd.DataFrame:
    idx = pd.date_range("2026-01-01", periods=n, freq="B", tz="UTC")
    return pd.DataFrame({"close": 100.0}, index=idx)


def _cfg(tmp_path, *, macro_weight: float = 1.0, min_coverage: float = 0.5) -> dict:
    return {
        "enabled": True,
        "offline_only": True,
        "strict_mode": False,
        "storage": {"root": str(tmp_path / "altdata")},
        "asof": {"tolerance_seconds": {"1D": 3_888_000}},
        "weights": {
            "base": {"macro": macro_weight},
            "quality": {"min_coverage": min_coverage},
        },
        "sources": {
            "fred": {"enabled": True, "api_key_env": "FRED_API_KEY", "series": ["FEDFUNDS"]},
            "edgar": {"enabled": False},
        },
    }


def _run_with_coverage(tmp_path, macro_coverage_value: float):
    """Run build_altdata_features with FRED mocked to produce a given macro coverage."""
    from octa.core.features.transforms.feature_builder import build_altdata_features

    bars = _make_bars()
    idx = bars.index

    # FRED snapshot payload (so offline path proceeds past missing_cache check)
    snapshot = {"series": {"FEDFUNDS": [
        {"ts": "2025-12-31T00:00:00+00:00", "value": 5.33,
         "as_of": None, "source_time": None, "ingested_at": None}
    ]}}

    # Macro features DataFrame — non-empty so feat_parts.append fires
    macro_df = pd.DataFrame({"FEDFUNDS_lag1": [5.0]})  # numeric index → ts col after reset_index

    # asof_join result controls coverage: NaN → coverage=0.0; numeric → coverage=1.0
    fedfunds_vals = macro_coverage_value if macro_coverage_value > 0.0 else float("nan")
    j_return = pd.DataFrame(
        {"ts": pd.Series(idx, dtype="datetime64[ns, UTC]"), "FEDFUNDS_lag1": fedfunds_vals},
        index=idx,
    )
    leak_none = pd.Series([False] * len(idx), index=idx)

    wide_df = pd.DataFrame(
        {"FEDFUNDS": [5.33]}, index=pd.DatetimeIndex(["2025-12-31"], tz="UTC")
    )

    with (
        patch("octa.core.features.transforms.feature_builder.read_snapshot", return_value=snapshot),
        patch("octa.core.features.transforms.feature_builder.fred_to_wide", return_value=wide_df),
        patch("octa.core.features.transforms.feature_builder.build_macro_features", return_value=macro_df),
        patch("octa.core.features.transforms.feature_builder.asof_join", return_value=j_return),
        patch(
            "octa.core.features.transforms.feature_builder.validate_no_future_leakage",
            return_value=leak_none,
        ),
    ):
        return build_altdata_features(
            bars_df=bars,
            symbol="TEST",
            altdat_cfg=_cfg(tmp_path, macro_weight=1.0, min_coverage=0.5),
        )


def test_altdata_zero_coverage_macro_excluded(tmp_path: "Path") -> None:
    """A4: coverage=0.0 → apply_quality_adjustments zeroes weight → no altdat_macro_* in output."""
    result = _run_with_coverage(tmp_path, macro_coverage_value=0.0)
    cols = list(result.features_df.columns)
    assert not any(c.startswith("altdat_macro_") for c in cols), (
        f"Expected no altdat_macro_* cols when coverage=0.0 (weight zeroed), got: "
        f"{[c for c in cols if 'macro' in c]}"
    )
    # Meta should record the final weight as 0.0
    assert result.meta["weights"]["final"].get("macro", 1.0) == 0.0


def test_altdata_full_coverage_macro_included(tmp_path: "Path") -> None:
    """A4 positive: coverage=1.0 → weight survives → altdat_macro_* present in output."""
    result = _run_with_coverage(tmp_path, macro_coverage_value=5.33)
    cols = list(result.features_df.columns)
    assert any(c.startswith("altdat_macro_") for c in cols), (
        f"Expected altdat_macro_* cols when coverage=1.0, got columns: {cols}"
    )
    # Weight should be non-zero
    w = result.meta["weights"]["final"].get("macro", 0.0)
    assert w > 0.0, f"Expected macro weight > 0.0 for full-coverage source, got {w}"
