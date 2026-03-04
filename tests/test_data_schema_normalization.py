"""Tests for Phase 3 B3: Data Schema Normalization.

Verifies:
1) test_parquet_time_string_is_parsed          — string datetime column → UTC DatetimeIndex
2) test_parquet_time_naive_ts_is_localized_to_utc — naive datetime[ns] → localized to UTC
3) test_parquet_time_tzaware_is_accepted        — tz-aware datetime[ns, UTC] accepted unchanged
4) test_index_based_parquet_datetimeindex_is_normalized — DatetimeIndex as parquet index → UTC
5) test_loader_missing_time_column_fails_closed — no time column → ValueError (fail-closed)
6) test_fx_spacing_weekend_tolerant_passes      — FX 1D Fri→Mon gap → DQ PASS
7) test_equity_spacing_still_strict             — equity 1H off-grid → DQ FAIL (regression guard)
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from octa.core.data.io.io_parquet import load_parquet
from octa_ops.autopilot.data_quality import DataQualityPolicy, evaluate_data_quality


# ---------------------------------------------------------------------------
# 1 — string time column → UTC DatetimeIndex
# ---------------------------------------------------------------------------

def test_parquet_time_string_is_parsed(tmp_path: Path) -> None:
    """String 'datetime' column must be parsed to UTC DatetimeIndex."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    p = tmp_path / "string_time.parquet"
    strings = ["2026-01-01 00:00:00", "2026-01-02 00:00:00", "2026-01-03 00:00:00",
               "2026-01-04 00:00:00", "2026-01-05 00:00:00"]
    table = pa.table({
        "datetime": pa.array(strings, type=pa.string()),
        "close":    pa.array([1.0, 1.0, 1.0, 1.0, 1.0]),
    })
    pq.write_table(table, str(p))

    df = load_parquet(p)
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz is not None
    assert str(df.index.tz).upper() in ("UTC", "UTC+00:00", "+00:00")
    assert len(df) == 5


# ---------------------------------------------------------------------------
# 2 — naive datetime[ns] → localized to UTC
# ---------------------------------------------------------------------------

def test_parquet_time_naive_ts_is_localized_to_utc(tmp_path: Path) -> None:
    """Naive datetime[ns] column must be localized to UTC (not rejected)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    p = tmp_path / "naive_time.parquet"
    # pa.timestamp("ns") without tz → naive datetime
    base_ns = 1735689600_000_000_000   # 2026-01-01 00:00:00 UTC
    step_ns = 86_400_000_000_000
    ts_ns = [base_ns + i * step_ns for i in range(5)]
    table = pa.table({
        "datetime": pa.array(ts_ns, type=pa.timestamp("ns")),
        "close":    pa.array([1.0, 1.0, 1.0, 1.0, 1.0]),
    })
    pq.write_table(table, str(p))

    df = load_parquet(p)
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz is not None
    assert str(df.index.tz).upper() in ("UTC", "UTC+00:00", "+00:00")
    assert len(df) == 5


# ---------------------------------------------------------------------------
# 3 — tz-aware UTC → passed through
# ---------------------------------------------------------------------------

def test_parquet_time_tzaware_is_accepted(tmp_path: Path) -> None:
    """datetime[ns, tz=UTC] must be accepted and remain UTC."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    p = tmp_path / "tzaware_time.parquet"
    base_ns = 1735689600_000_000_000
    step_ns = 86_400_000_000_000
    ts_ns = [base_ns + i * step_ns for i in range(5)]
    table = pa.table({
        "datetime": pa.array(ts_ns, type=pa.timestamp("ns", tz="UTC")),
        "close":    pa.array([1.0, 1.0, 1.0, 1.0, 1.0]),
    })
    pq.write_table(table, str(p))

    df = load_parquet(p)
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz is not None
    assert str(df.index.tz).upper() in ("UTC", "UTC+00:00", "+00:00")
    assert len(df) == 5


# ---------------------------------------------------------------------------
# 4 — index-based parquet → DatetimeIndex normalized to UTC
# ---------------------------------------------------------------------------

def test_index_based_parquet_datetimeindex_is_normalized(tmp_path: Path) -> None:
    """Parquet written with DatetimeIndex as index (no separate column) must load correctly."""
    p = tmp_path / "index_time.parquet"
    idx = pd.date_range("2026-01-01", periods=5, freq="D", tz="UTC")
    df = pd.DataFrame({"close": [1.0, 1.0, 1.0, 1.0, 1.0]}, index=idx)
    df.to_parquet(p, index=True)

    out = load_parquet(p)
    assert isinstance(out.index, pd.DatetimeIndex)
    assert out.index.tz is not None
    assert str(out.index.tz).upper() in ("UTC", "UTC+00:00", "+00:00")
    assert len(out) == 5


# ---------------------------------------------------------------------------
# 5 — missing time column → fail-closed
# ---------------------------------------------------------------------------

def test_loader_missing_time_column_fails_closed(tmp_path: Path) -> None:
    """Parquet with 'close' but no recognised time column must raise ValueError."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    p = tmp_path / "missing_time.parquet"
    # bar_id is not a recognised time column name
    table = pa.table({
        "bar_id": pa.array([1, 2, 3]),
        "close":  pa.array([1.0, 1.0, 1.0]),
    })
    pq.write_table(table, str(p))

    with pytest.raises(ValueError, match="missing time column"):
        load_parquet(p)


# ---------------------------------------------------------------------------
# 6 — FX 1D weekend gaps → DQ PASS
# ---------------------------------------------------------------------------

def test_fx_spacing_weekend_tolerant_passes(tmp_path: Path) -> None:
    """FX 1D with Fri→Mon weekend gaps must pass DQ spacing (FX-specific profile)."""
    p = tmp_path / "EURUSD_1D.parquet"
    ts = pd.to_datetime([
        "2026-01-02T00:00:00Z",   # Fri
        "2026-01-05T00:00:00Z",   # Mon  ← 3-day weekend gap
        "2026-01-06T00:00:00Z",   # Tue
        "2026-01-07T00:00:00Z",   # Wed
        "2026-01-08T00:00:00Z",   # Thu
        "2026-01-09T00:00:00Z",   # Fri
        "2026-01-12T00:00:00Z",   # Mon  ← 3-day weekend gap
        "2026-01-13T00:00:00Z",   # Tue
        "2026-01-14T00:00:00Z",   # Wed
        "2026-01-15T00:00:00Z",   # Thu
    ], utc=True)
    df = pd.DataFrame({"timestamp": ts, "close": 1.0})
    df.to_parquet(p, index=False)

    dec = evaluate_data_quality(
        symbol="EURUSD",
        timeframe="1D",
        parquet_path=str(p),
        asset_class="forex",
        policy=DataQualityPolicy(),
    )
    assert dec.status == "PASS", (
        f"FX 1D with weekend gaps must PASS DQ, got {dec.status!r}: "
        f"reason={dec.reason} details={dec.details}"
    )
    # Verify the FX weekend-tolerant branch was actually used
    assert "fx_1d_weekend_gaps_excluded" in (dec.details or {}), (
        "DQ details must record fx_1d_weekend_gaps_excluded when FX-1D branch fires"
    )
    assert dec.details["fx_1d_weekend_gaps_excluded"] >= 1


# ---------------------------------------------------------------------------
# 7 — equity 1H off-grid → DQ FAIL (regression guard)
# ---------------------------------------------------------------------------

def test_equity_spacing_still_strict(tmp_path: Path) -> None:
    """Equity 1H DQ must still reject off-grid spacing (FX fix must not weaken equity)."""
    p = tmp_path / "AAPL_1H.parquet"
    ts = pd.to_datetime([
        "2026-01-01T10:00:00Z",
        "2026-01-01T10:30:00Z",   # off-grid 30-min bar
        "2026-01-01T11:00:00Z",
        "2026-01-01T12:00:00Z",
        "2026-01-01T13:00:00Z",
    ], utc=True)
    df = pd.DataFrame({"timestamp": ts, "close": 1.0})
    df.to_parquet(p, index=False)

    dec = evaluate_data_quality(
        symbol="AAPL",
        timeframe="1H",
        parquet_path=str(p),
        asset_class="equity",
        policy=DataQualityPolicy(),
    )
    assert dec.status == "FAIL", (
        f"Equity 1H with off-grid bars must FAIL, got {dec.status!r}: reason={dec.reason}"
    )
    assert dec.reason == "spacing_not_timeframe_like"
