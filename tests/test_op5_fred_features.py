"""OP-5: FRED numerische Features — Integration Tests."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from types import SimpleNamespace

from octa.core.features.features import build_features

DUCKDB_PATH = "octa/var/altdata/altdat.duckdb"
DUCKDB_AVAILABLE = Path(DUCKDB_PATH).exists()


def _macro_cfg(path: str = DUCKDB_PATH) -> dict:
    return {
        "enabled": True,
        "source": "duckdb",
        "altdat_duckdb_path": path,
        "series": ["FEDFUNDS", "DGS10", "DGS2", "UNRATE"],
        "shift_bars": 1,
    }


def _settings(macro: dict) -> SimpleNamespace:
    return SimpleNamespace(
        raw_dir=None,
        timeframe="1D",
        features={},
        macro=macro,
        symbol="TEST",
        run_id="test",
    )


def _price_df(start: str = "2020-01-02", end: str = "2023-12-29") -> pd.DataFrame:
    idx = pd.date_range(start, end, freq="B", tz="UTC")
    n = len(idx)
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "open": 100 + rng.normal(0, 1, n).cumsum(),
            "high": 101 + rng.normal(0, 1, n).cumsum(),
            "low": 99 + rng.normal(0, 1, n).cumsum(),
            "close": 100 + rng.normal(0, 1, n).cumsum(),
            "volume": np.abs(rng.normal(1e6, 1e5, n)),
        },
        index=idx,
    )


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="altdat.duckdb not present")
def test_fred_features_present_in_build_features() -> None:
    """build_features() must include macro_ columns when DuckDB source is configured."""
    res = build_features(_price_df(), _settings(_macro_cfg()), asset_class="stock")
    macro_cols = [c for c in res.X.columns if c.startswith("macro_")]
    assert len(macro_cols) >= 4, f"Expected ≥4 macro cols, got {macro_cols}"
    assert "macro_FEDFUNDS" in res.X.columns
    assert "macro_DGS10" in res.X.columns
    assert "macro_DGS2" in res.X.columns


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="altdat.duckdb not present")
def test_fred_yield_curve_10_2_present() -> None:
    """Derived YIELD_CURVE_10_2 (DGS10 - DGS2) must be in features."""
    res = build_features(_price_df(), _settings(_macro_cfg()), asset_class="stock")
    assert "macro_YIELD_CURVE_10_2" in res.X.columns


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="altdat.duckdb not present")
def test_fred_features_no_nan() -> None:
    """Macro features must have no NaN after ffill (except possibly first bars)."""
    res = build_features(_price_df(), _settings(_macro_cfg()), asset_class="stock")
    macro_cols = [c for c in res.X.columns if c.startswith("macro_")]
    if not macro_cols:
        pytest.skip("No macro cols")
    nan_rate = res.X[macro_cols].isna().mean().max()
    assert nan_rate < 0.01, f"Max NaN rate in macro cols: {nan_rate:.3f}"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="altdat.duckdb not present")
def test_fred_features_index_matches() -> None:
    """Feature matrix index must match the input price index."""
    df = _price_df()
    res = build_features(df, _settings(_macro_cfg()), asset_class="stock")
    # X index is a subset of df index (some rows dropped due to NaN targets)
    assert set(res.X.index).issubset(set(df.index))


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="altdat.duckdb not present")
def test_fred_temporal_consistency() -> None:
    """2022 FEDFUNDS level must exceed 2020 level (Fed hiking cycle check — no lookahead)."""
    res = build_features(_price_df(), _settings(_macro_cfg()), asset_class="stock")
    if "macro_FEDFUNDS" not in res.X.columns:
        pytest.skip("macro_FEDFUNDS not in features")
    # Use nearest available business days
    col = "macro_FEDFUNDS"
    v2020 = res.X[col].asof(pd.Timestamp("2020-06-15", tz="UTC"))
    v2022 = res.X[col].asof(pd.Timestamp("2022-12-15", tz="UTC"))
    assert v2022 > v2020, (
        f"TEMPORAL CONSISTENCY FAIL: FEDFUNDS 2022-12={v2022:.4f} <= 2020-06={v2020:.4f}"
    )


def test_fred_graceful_degradation_missing_db() -> None:
    """Missing DuckDB path must keep deterministic macro columns with neutral fallback."""
    res = build_features(
        _price_df("2022-01-03", "2022-12-30"),
        _settings(_macro_cfg("/nonexistent/path.duckdb")),
        asset_class="stock",
    )
    macro_cols = [c for c in res.X.columns if c.startswith("macro_")]
    assert "macro_FEDFUNDS" in macro_cols
    assert "macro_DGS10" in macro_cols
    assert "macro_DGS2" in macro_cols
    assert res.meta.get("macro_meta", {}).get("degraded") is True


def test_fred_disabled_macro_produces_no_macro_cols() -> None:
    """enabled: false must produce no macro_ columns."""
    cfg = _macro_cfg()
    cfg["enabled"] = False
    res = build_features(_price_df("2022-01-03", "2022-12-30"), _settings(cfg), asset_class="stock")
    macro_cols = [c for c in res.X.columns if c.startswith("macro_")]
    assert macro_cols == []
