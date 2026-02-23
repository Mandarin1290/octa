from __future__ import annotations

from pathlib import Path

import pandas as pd

from octa_ops.autopilot.data_quality import DataQualityPolicy, evaluate_data_quality


def test_data_quality_passes_clean_hourly(tmp_path: Path):
    p = tmp_path / "FOO_1H.parquet"
    ts = pd.date_range("2024-01-01", periods=50, freq="h", tz="UTC")
    df = pd.DataFrame({"timestamp": ts, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})
    df.to_parquet(p, index=False)

    dec = evaluate_data_quality(symbol="FOO", timeframe="1H", parquet_path=str(p), asset_class="fx", policy=DataQualityPolicy())
    assert dec.status == "PASS"


def test_data_quality_fails_duplicates(tmp_path: Path):
    p = tmp_path / "FOO_1H.parquet"
    # Irregular spacing: introduce off-grid 30m steps in an hourly series.
    ts = pd.date_range("2024-01-01", periods=10, freq="h", tz="UTC")
    ts = ts.insert(5, ts[5] + pd.Timedelta(minutes=30))
    df = pd.DataFrame({"timestamp": ts, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})
    df.to_parquet(p, index=False)

    pol = DataQualityPolicy(min_spacing_match_frac=1.0)
    dec = evaluate_data_quality(symbol="FOO", timeframe="1H", parquet_path=str(p), asset_class="fx", policy=pol)
    assert dec.status == "FAIL"


def test_data_quality_allows_1d_equity_weekend_gaps(tmp_path: Path):
    p = tmp_path / "EQ_1D.parquet"
    ts = pd.to_datetime(
        [
            "2024-01-05T00:00:00Z",  # Fri
            "2024-01-08T00:00:00Z",  # Mon (3-day gap)
            "2024-01-09T00:00:00Z",
            "2024-01-10T00:00:00Z",
        ],
        utc=True,
    )
    df = pd.DataFrame({"timestamp": ts, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})
    df.to_parquet(p, index=False)

    dec = evaluate_data_quality(symbol="EQ", timeframe="1D", parquet_path=str(p), asset_class="equity", policy=DataQualityPolicy())
    assert dec.status == "PASS"


def test_data_quality_allows_1h_equity_overnight_gaps(tmp_path: Path):
    p = tmp_path / "EQ_1H.parquet"
    ts = pd.to_datetime(
        [
            "2024-01-02T14:00:00Z",
            "2024-01-02T15:00:00Z",
            "2024-01-02T16:00:00Z",
            "2024-01-03T14:00:00Z",
            "2024-01-03T15:00:00Z",
            "2024-01-03T16:00:00Z",
        ],
        utc=True,
    )
    df = pd.DataFrame({"timestamp": ts, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})
    df.to_parquet(p, index=False)

    dec = evaluate_data_quality(symbol="EQ", timeframe="1H", parquet_path=str(p), asset_class="equity", policy=DataQualityPolicy())
    assert dec.status == "PASS"


def test_data_quality_still_fails_1h_equity_intraday_offgrid(tmp_path: Path):
    p = tmp_path / "EQ_1H.parquet"
    ts = pd.to_datetime(
        [
            "2024-01-02T14:00:00Z",
            "2024-01-02T14:30:00Z",
            "2024-01-02T15:00:00Z",
            "2024-01-02T16:00:00Z",
            "2024-01-03T14:00:00Z",
        ],
        utc=True,
    )
    df = pd.DataFrame({"timestamp": ts, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})
    df.to_parquet(p, index=False)

    dec = evaluate_data_quality(symbol="EQ", timeframe="1H", parquet_path=str(p), asset_class="equity", policy=DataQualityPolicy())
    assert dec.status == "FAIL"
