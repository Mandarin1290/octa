"""Tests for B6: Futures 1D regeneration from intraday.

Verifies:
1) resample_to_1d deterministic: same input → identical output hash
2) integrity check passes for regenerated 1D
3) no forward-fill: missing UTC days absent from output
4) build_symbol_1d: corrupt file moved, new file written, manifest OK
5) ED (no intraday) → RuntimeError NO_INTRADAY_SOURCE
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from octa.core.data.builders.futures_1d_regen import (
    build_symbol_1d,
    load_intraday,
    resample_to_1d,
    write_manifest_entry,
)
from octa.core.data.io.timeseries_integrity import validate_timeseries_integrity


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_intraday_df(
    n_days: int = 60,
    hours_per_day: int = 23,
    *,
    tz: str = "UTC",
) -> pd.DataFrame:
    """Build a synthetic hourly OHLCV DataFrame with UTC DatetimeIndex."""
    rng = np.random.default_rng(42)
    dates = pd.date_range("2020-01-01", periods=n_days, freq="B", tz=tz)
    rows = []
    for d in dates:
        for h in range(hours_per_day):
            ts = d + pd.Timedelta(hours=h)
            p = rng.uniform(100.0, 200.0)
            rows.append({"datetime": ts, "open": p, "high": p * 1.01, "low": p * 0.99, "close": p * 1.005, "volume": int(rng.integers(1000, 5000))})
    df = pd.DataFrame(rows).set_index("datetime")
    return df


def _make_naive_intraday_parquet(path: Path, n_days: int = 60) -> None:
    """Write a parquet with naive (no-tz) DatetimeIndex — like real Futures_Parquet files."""
    df = _make_intraday_df(n_days=n_days)
    df.index = df.index.tz_localize(None)  # strip tz → naive
    df.to_parquet(str(path))


def _make_corrupt_1d_parquet(path: Path) -> None:
    """Write a parquet with corrupt index (price floats as strings)."""
    rng = np.random.default_rng(99)
    prices = rng.uniform(0.5, 2.0, 50)
    idx = pd.Index([f"{p:.5f}" for p in prices], name="datetime")
    df = pd.DataFrame(
        {"open": prices, "high": prices * 1.01, "low": prices * 0.99,
         "close": rng.integers(10_000, 50_000, 50), "volume": rng.integers(100, 1_000, 50)},
        index=idx,
    )
    df.to_parquet(str(path))


# ---------------------------------------------------------------------------
# 1 — resample_to_1d deterministic
# ---------------------------------------------------------------------------

def test_resample_deterministic() -> None:
    """Same intraday input → identical SHA-256 of resampled output."""
    df = _make_intraday_df(n_days=30)

    df1 = resample_to_1d(df)
    df2 = resample_to_1d(df.copy())

    # Serialize to bytes and hash
    def _hash(d: pd.DataFrame) -> str:
        return hashlib.sha256(d.to_parquet()).hexdigest()

    assert _hash(df1) == _hash(df2), "resample_to_1d must be deterministic"


# ---------------------------------------------------------------------------
# 2 — integrity check passes for generated 1D
# ---------------------------------------------------------------------------

def test_regen_integrity_passes() -> None:
    """resample_to_1d output passes validate_timeseries_integrity."""
    df_intraday = _make_intraday_df(n_days=60)
    df_1d = resample_to_1d(df_intraday)

    ok, reason, details = validate_timeseries_integrity(
        df_1d, "futures", "1D", "/fake/X_1D.parquet"
    )
    assert ok, f"Integrity check failed: {reason!r}\ndetails={details}"
    assert reason == ""


# ---------------------------------------------------------------------------
# 3 — no forward-fill: gaps preserved
# ---------------------------------------------------------------------------

def test_no_forward_fill_gaps_preserved() -> None:
    """Missing UTC day buckets remain absent — no forward-fill."""
    # Build 5 hourly bars all on 2020-01-02; 2020-01-03 has no data
    idx = pd.DatetimeIndex([
        "2020-01-02 06:00", "2020-01-02 07:00", "2020-01-02 08:00",
        "2020-01-02 09:00", "2020-01-02 10:00",
        "2020-01-04 06:00", "2020-01-04 07:00", "2020-01-04 08:00",
        "2020-01-04 09:00", "2020-01-04 10:00",
    ], tz="UTC")
    df = pd.DataFrame(
        {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000},
        index=idx,
    )
    df_1d = resample_to_1d(df)
    dates_out = set(str(d.date()) for d in df_1d.index)
    assert "2020-01-02" in dates_out, "2020-01-02 must be present"
    assert "2020-01-03" not in dates_out, "2020-01-03 must be absent (no forward-fill)"
    assert "2020-01-04" in dates_out, "2020-01-04 must be present"


# ---------------------------------------------------------------------------
# 4 — build_symbol_1d: end-to-end
# ---------------------------------------------------------------------------

def test_build_symbol_1d_produces_valid_output(tmp_path: Path) -> None:
    """build_symbol_1d: corrupt 1D moved, new 1D written, manifest OK."""
    sym = "TST"
    futures_dir = tmp_path / "futures"
    futures_dir.mkdir()
    corrupt_dir = tmp_path / "corrupt"
    evdir = tmp_path / "ev"

    # Write intraday source (1H)
    _make_naive_intraday_parquet(futures_dir / f"{sym}_1H.parquet", n_days=60)
    # Write corrupt 1D (to be moved)
    _make_corrupt_1d_parquet(futures_dir / f"{sym}_1D.parquet")
    corrupt_sha_before = hashlib.sha256((futures_dir / f"{sym}_1D.parquet").read_bytes()).hexdigest()

    manifest = build_symbol_1d(
        sym,
        futures_dir=futures_dir,
        output_dir=futures_dir,
        corrupt_dir=corrupt_dir,
    )

    # New 1D file must exist and pass integrity
    new_1d = futures_dir / f"{sym}_1D.parquet"
    assert new_1d.exists(), "New 1D parquet must be written"
    df_out = pd.read_parquet(str(new_1d))
    ok, reason, _ = validate_timeseries_integrity(df_out, "futures", "1D", str(new_1d))
    assert ok, f"Regenerated 1D fails integrity: {reason!r}"

    # Corrupt file must be moved
    moved = corrupt_dir / f"{sym}_1D.parquet"
    assert moved.exists(), "Corrupt file must be moved to corrupt_dir"
    assert hashlib.sha256(moved.read_bytes()).hexdigest() == corrupt_sha_before, \
        "Moved file must have the same sha256 as the original corrupt file"

    # Manifest must record OK
    assert manifest["status"] == "OK"
    assert manifest["source_tf"] == "1H"
    assert manifest["output_rows"] >= 20
    assert len(manifest["output_sha256"]) == 64
    assert len(manifest["source_sha256"]) == 64


# ---------------------------------------------------------------------------
# 5 — ED-like: no intraday → RuntimeError
# ---------------------------------------------------------------------------

def test_no_intraday_source_raises(tmp_path: Path) -> None:
    """Symbol with only corrupt 1D and no intraday → NO_INTRADAY_SOURCE."""
    futures_dir = tmp_path / "futures"
    futures_dir.mkdir()
    sym = "ED"
    _make_corrupt_1d_parquet(futures_dir / f"{sym}_1D.parquet")
    # No 1H/30M/5M files written

    with pytest.raises(RuntimeError, match="NO_INTRADAY_SOURCE"):
        build_symbol_1d(sym, futures_dir=futures_dir, output_dir=futures_dir)
