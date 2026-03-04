"""Tests for B5: Futures 1D data integrity validation and quarantine gate.

Verifies:
1) validate_timeseries_integrity — corrupt df (object index, price floats)
   → ok=False, FUTURES_1D_CORRUPT_DATA:INDEX_CONTAINS_PRICE_FLOATS
2) validate_timeseries_integrity — good df (DatetimeIndex, numeric OHLCV)
   → ok=True
3) evaluate_data_quality Futures 1D — corrupt parquet
   → GateDecision FAIL with reason starting FUTURES_1D_CORRUPT_DATA
4) quarantine_manifest.jsonl written when quarantine_dir is supplied
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from octa.core.data.io.timeseries_integrity import validate_timeseries_integrity
from octa_ops.autopilot.data_quality import DataQualityPolicy, evaluate_data_quality


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_corrupt_df(n: int = 50) -> pd.DataFrame:
    """DataFrame mimicking real Futures 1D corruption:
    - Index: object dtype containing price float strings (NOT DatetimeIndex)
    - open/high/low: float64 (price data)
    - close: int64 (volume-like data, NOT prices)
    - volume: int64
    """
    rng = np.random.default_rng(42)
    prices = rng.uniform(0.5, 2.0, n)
    index = pd.Index([f"{p:.5f}" for p in prices], name="datetime")
    return pd.DataFrame(
        {
            "open": prices,
            "high": prices * 1.01,
            "low": prices * 0.99,
            "close": rng.integers(10_000, 50_000, n),
            "volume": rng.integers(100, 1_000, n),
        },
        index=index,
    )


def _make_good_df(n: int = 50) -> pd.DataFrame:
    """DataFrame with proper DatetimeIndex and numeric OHLCV."""
    idx = pd.date_range("2020-01-01", periods=n, freq="B", tz="UTC")
    rng = np.random.default_rng(0)
    prices = rng.uniform(100.0, 200.0, n)
    return pd.DataFrame(
        {
            "open": prices,
            "high": prices * 1.01,
            "low": prices * 0.99,
            "close": prices * 1.005,
            "volume": rng.integers(1_000, 10_000, n).astype(float),
        },
        index=idx,
    )


def _write_corrupt_parquet(path: Path, n: int = 50) -> None:
    """Write a corrupt Futures-1D-style parquet to *path*."""
    _make_corrupt_df(n).to_parquet(path)


def _write_good_parquet(path: Path, n: int = 50) -> None:
    """Write a valid Futures-1D-style parquet to *path*."""
    _make_good_df(n).to_parquet(path)


# ---------------------------------------------------------------------------
# 1 — corrupt DataFrame → FUTURES_1D_CORRUPT_DATA
# ---------------------------------------------------------------------------

def test_validate_integrity_corrupt_df_fails() -> None:
    """Object index with price floats → INDEX_CONTAINS_PRICE_FLOATS."""
    df = _make_corrupt_df()
    ok, reason, details = validate_timeseries_integrity(df, "futures", "1D", "/fake/A6_1D.parquet")

    assert not ok, f"Expected ok=False, got reason={reason!r}"
    assert reason.startswith("FUTURES_1D_CORRUPT_DATA:"), reason
    assert "INDEX_CONTAINS_PRICE_FLOATS" in reason or "INDEX_NOT_DATETIME" in reason
    assert details["index_dtype"] == "object"
    assert details["nrows"] == 50


# ---------------------------------------------------------------------------
# 2 — good DataFrame → ok=True
# ---------------------------------------------------------------------------

def test_validate_integrity_good_df_passes() -> None:
    """DatetimeIndex + numeric OHLCV → ok=True."""
    df = _make_good_df()
    ok, reason, details = validate_timeseries_integrity(df, "futures", "1D", "/fake/GOOD_1D.parquet")

    assert ok, f"Expected ok=True, got reason={reason!r}"
    assert reason == ""
    assert details["nrows"] == 50


# ---------------------------------------------------------------------------
# 3 — DQ gate for Futures 1D corrupt parquet → FAIL + specific reason
# ---------------------------------------------------------------------------

def test_dq_gate_futures_1d_corrupt_emits_specific_reason(tmp_path: Path) -> None:
    """evaluate_data_quality for futures+1D corrupt parquet → FAIL, FUTURES_1D_CORRUPT_DATA."""
    pq_path = tmp_path / "A6_1D.parquet"
    _write_corrupt_parquet(pq_path)

    policy = DataQualityPolicy()
    decision = evaluate_data_quality(
        symbol="A6",
        timeframe="1D",
        parquet_path=str(pq_path),
        asset_class="futures",
        policy=policy,
    )

    assert decision.status == "FAIL", f"Expected FAIL, got {decision.status!r}"
    assert decision.reason is not None and decision.reason.startswith(
        "FUTURES_1D_CORRUPT_DATA:"
    ), f"Expected FUTURES_1D_CORRUPT_DATA:* reason, got {decision.reason!r}"
    # Must NOT be the generic data_load_failed (that would mean we didn't catch it early)
    assert decision.reason != "data_load_failed"


# ---------------------------------------------------------------------------
# 4 — quarantine manifest written when quarantine_dir supplied
# ---------------------------------------------------------------------------

def test_quarantine_manifest_written_on_corrupt(tmp_path: Path) -> None:
    """Corrupt Futures 1D + quarantine_dir → quarantine_manifest.jsonl entry written."""
    pq_path = tmp_path / "CL_1D.parquet"
    _write_corrupt_parquet(pq_path)
    q_dir = tmp_path / "quarantine"

    policy = DataQualityPolicy()
    decision = evaluate_data_quality(
        symbol="CL",
        timeframe="1D",
        parquet_path=str(pq_path),
        asset_class="futures",
        policy=policy,
        quarantine_dir=q_dir,
    )

    assert decision.status == "FAIL"
    manifest = q_dir / "quarantine_manifest.jsonl"
    assert manifest.exists(), "quarantine_manifest.jsonl must be created"

    entries = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(entries) == 1, f"Expected 1 quarantine entry, got {len(entries)}"
    e = entries[0]
    assert e["asset_class"] == "futures"
    assert e["timeframe"] == "1D"
    assert e["reason"].startswith("FUTURES_1D_CORRUPT_DATA:")
    assert e["path"] == str(pq_path)
    assert len(e["sha256"]) == 64  # sha256 hex digest


# ---------------------------------------------------------------------------
# 5 — non-Futures asset class bypasses the pre-flight check
# ---------------------------------------------------------------------------

def test_non_futures_asset_class_bypasses_preflight(tmp_path: Path) -> None:
    """equity 1D with corrupt-looking index → NOT caught by Futures pre-flight.
    Falls through to generic data_load_failed (load_parquet raises ValueError).
    """
    pq_path = tmp_path / "SPY_1D.parquet"
    _write_corrupt_parquet(pq_path)

    policy = DataQualityPolicy()
    decision = evaluate_data_quality(
        symbol="SPY",
        timeframe="1D",
        parquet_path=str(pq_path),
        asset_class="equity",
        policy=policy,
    )

    # load_parquet will fail because the parquet has no DatetimeIndex and no
    # recognizable time column; the DQ gate returns data_load_failed or
    # timestamp_not_datetimeindex — NOT FUTURES_1D_CORRUPT_DATA.
    assert decision.status == "FAIL"
    assert not (decision.reason or "").startswith("FUTURES_1D_CORRUPT_DATA:")
