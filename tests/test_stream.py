from pathlib import Path

import pandas as pd

from octa_stream.lineage import parquet_content_hash
from octa_stream.manifest import AssetManifest
from octa_stream.validate import ParquetValidator


def _write_parquet(tmp_path: Path, df: pd.DataFrame, fname: str) -> str:
    p = tmp_path / fname
    df.to_parquet(p, index=False)
    return str(p)


def test_invalid_schema_rejected(tmp_path):
    # missing 'open' column
    df = pd.DataFrame({"timestamp": ["2020-01-01T00:00:00Z"], "close": [1.0]})
    p = _write_parquet(tmp_path, df, "bad.parquet")
    manifest = AssetManifest(
        asset_id="a1",
        symbol="SYM",
        asset_class="EQUITY",
        venue="V",
        currency="USD",
        parquet_path=p,
    )
    v = ParquetValidator()
    res = v.validate(manifest)
    assert not res.eligible


def test_time_duplicates_rejected(tmp_path):
    df = pd.DataFrame(
        {
            "timestamp": ["2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"],
            "open": [1.0, 1.0],
            "high": [1.1, 1.1],
            "low": [0.9, 0.9],
            "close": [1.0, 1.0],
            "volume": [100, 100],
        }
    )
    p = _write_parquet(tmp_path, df, "dup.parquet")
    manifest = AssetManifest(
        asset_id="a2",
        symbol="SYM2",
        asset_class="EQUITY",
        venue="V",
        currency="USD",
        parquet_path=p,
    )
    v = ParquetValidator()
    res = v.validate(manifest)
    assert not res.eligible


def test_fx_volume_optional(tmp_path):
    df = pd.DataFrame(
        {
            "timestamp": ["2020-01-01T00:00:00Z"],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [None],
        }
    )
    p = _write_parquet(tmp_path, df, "fx.parquet")
    manifest = AssetManifest(
        asset_id="fx1",
        symbol="EURUSD",
        asset_class="FX",
        venue="FX",
        currency="USD",
        parquet_path=p,
    )
    v = ParquetValidator()
    res = v.validate(manifest)
    assert res.eligible


def test_lineage_hash_stable(tmp_path):
    df = pd.DataFrame(
        {
            "timestamp": ["2020-01-01T00:00:00Z"],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [10],
        }
    )
    p = _write_parquet(tmp_path, df, "h.parquet")
    h1 = parquet_content_hash(p)
    h2 = parquet_content_hash(p)
    assert h1 == h2
