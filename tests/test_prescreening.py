"""Tests for octa_training.core.prescreening (v0.0.0)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from octa_training.core.prescreening import (
    ScreenResult,
    prescreen_symbol,
    prescreen_universe,
    REASON_INSUFFICIENT_HISTORY,
    REASON_PRICE_TOO_LOW,
    REASON_VOLUME_TOO_LOW,
    REASON_WARRANT_OR_RIGHTS,
    REASON_INSUFFICIENT_REGIME_DIVERSITY,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(
    n: int = 600,
    price: float = 50.0,
    volume: float = 500_000,
) -> pd.DataFrame:
    """Create a valid DataFrame that passes all filters by default."""
    rng = np.random.default_rng(42)
    close = price * np.cumprod(1 + rng.normal(0, 0.005, n))
    idx = pd.date_range("2018-01-01", periods=n, freq="B")
    return pd.DataFrame({
        "close": close,
        "open": close,
        "high": close * 1.001,
        "low": close * 0.999,
        "volume": volume,
    }, index=idx)


# ---------------------------------------------------------------------------
# Test (a): Sub-Dollar → PRESCREENED_OUT price_too_low
# ---------------------------------------------------------------------------

def test_price_too_low():
    df = _make_df(n=600, price=0.50)  # mean well below $1.00
    result = prescreen_symbol(df, "XXXX")
    assert not result.passed
    assert result.reason == REASON_PRICE_TOO_LOW
    assert "mean_price" in result.detail


# ---------------------------------------------------------------------------
# Test (b): Warrant suffix → PRESCREENED_OUT warrant_or_rights
# ---------------------------------------------------------------------------

def test_warrant_suffix_W():
    df = _make_df(n=600, price=15.0)
    result = prescreen_symbol(df, "ARQQW")
    assert not result.passed
    assert result.reason == REASON_WARRANT_OR_RIGHTS


def test_warrant_suffix_WS():
    df = _make_df(n=600, price=15.0)
    result = prescreen_symbol(df, "ABCDWS")
    assert not result.passed
    assert result.reason == REASON_WARRANT_OR_RIGHTS


def test_warrant_runs_before_df_load():
    """Warrant check should fire even when df=None (no parquet loaded)."""
    result = prescreen_symbol(None, "TESTW")
    assert not result.passed
    assert result.reason == REASON_WARRANT_OR_RIGHTS


# ---------------------------------------------------------------------------
# Test (c): Too-short history → PRESCREENED_OUT insufficient_history
# ---------------------------------------------------------------------------

def test_insufficient_history():
    df = _make_df(n=200, price=15.0)  # 200 < 504
    result = prescreen_symbol(df, "AAPL")
    assert not result.passed
    assert result.reason == REASON_INSUFFICIENT_HISTORY
    assert result.detail["n_rows"] == 200


def test_none_df_insufficient_history():
    """None df should fail with insufficient_history (after warrant check)."""
    result = prescreen_symbol(None, "AAPL")
    assert not result.passed
    assert result.reason == REASON_INSUFFICIENT_HISTORY


# ---------------------------------------------------------------------------
# Test (d): Valid symbol → passed=True
# ---------------------------------------------------------------------------

def test_valid_symbol_passes():
    df = _make_df(n=700, price=50.0, volume=500_000)
    result = prescreen_symbol(df, "AAPL")
    assert result.passed, f"Expected pass; got reason={result.reason}, detail={result.detail}"
    assert result.reason is None


# ---------------------------------------------------------------------------
# Test: Volume filter
# ---------------------------------------------------------------------------

def test_volume_too_low():
    df = _make_df(n=600, price=50.0, volume=500)  # 500 << 100_000
    result = prescreen_symbol(df, "AAPL")
    assert not result.passed
    assert result.reason == REASON_VOLUME_TOO_LOW
    assert result.detail["recent_vol_20d"] < 100_000


# ---------------------------------------------------------------------------
# Test: prescreen_universe summary logging
# ---------------------------------------------------------------------------

def test_prescreen_universe_summary(tmp_path):
    """prescreen_universe should return results for all symbols and log summary."""
    import tempfile
    from pathlib import Path

    # Write two parquets: one valid, one with short history
    valid_df = _make_df(n=700, price=50.0, volume=500_000)
    short_df = _make_df(n=200, price=50.0, volume=500_000)

    valid_path = str(tmp_path / "AAPL_1D.parquet")
    short_path = str(tmp_path / "ZZZZ_1D.parquet")
    valid_df.to_parquet(valid_path)
    short_df.to_parquet(short_path)

    inventory = {
        "AAPL": {"asset_class": "stock", "tfs": {"1D": [valid_path]}},
        "ZZZZ": {"asset_class": "stock", "tfs": {"1D": [short_path]}},
        "TESTW": {"asset_class": "stock", "tfs": {"1D": [valid_path]}},  # warrant
    }

    log_messages = []
    results = prescreen_universe(
        symbols=["AAPL", "ZZZZ", "TESTW"],
        inventory=inventory,
        cfg=None,
        log_fn=log_messages.append,
    )

    assert results["AAPL"].passed
    assert not results["ZZZZ"].passed
    assert results["ZZZZ"].reason == REASON_INSUFFICIENT_HISTORY
    assert not results["TESTW"].passed
    assert results["TESTW"].reason == REASON_WARRANT_OR_RIGHTS

    # Summary should have been logged
    assert len(log_messages) >= 1
    summary_msg = log_messages[-1]
    assert "passed" in summary_msg
    assert "failed" in summary_msg
