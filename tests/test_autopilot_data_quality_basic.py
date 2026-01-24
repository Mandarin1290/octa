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
