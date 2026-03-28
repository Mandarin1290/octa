import pickle
from pathlib import Path

import pytest

from octa_training.core.artifact_io import _compute_sha256_bytes, smoke_test_artifact


class DummyInfer:
    def predict(self, X):
        return {
            "signal": 0.0,
            "position": 0.0,
            "confidence": 0.0,
            "diagnostics": {},
        }


def test_smoke_test_finds_timeframe_suffixed_parquet(tmp_path: Path) -> None:
    """Regression: smoke_test_artifact must find SYMBOL_1H.parquet when artifact symbol is SYMBOL."""

    raw_dir = tmp_path / "raw"
    stock_dir = raw_dir / "Stock_parquet"
    stock_dir.mkdir(parents=True, exist_ok=True)

    # Minimal parquet with required schema.
    import pandas as pd

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=120, freq="h", tz="UTC"),
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
        }
    )
    parquet_path = stock_dir / "FOO_1H.parquet"
    df.to_parquet(parquet_path)

    artifact = {
        "schema_version": 1,
        "asset": {"symbol": "FOO", "asset_class": "stock", "bar_size": "1H"},
        "feature_spec": {"features": ["close"], "feature_config": {"feature_settings": {}}},
        "safe_inference": DummyInfer(),
    }

    pkl_bytes = pickle.dumps(artifact, protocol=4)
    pkl_path = tmp_path / "FOO.pkl"
    sha_path = tmp_path / "FOO.sha256"
    pkl_path.write_bytes(pkl_bytes)
    sha_path.write_text(_compute_sha256_bytes(pkl_bytes), encoding="utf-8")

    # Should not raise: parquet resolution must fallback to FOO_1H.
    out = smoke_test_artifact(str(pkl_path), str(raw_dir), last_n=5)
    assert out.get("symbol") == "FOO"


def test_smoke_test_infers_timeframe_from_pkl_path(tmp_path: Path) -> None:
    """Regression: when asset.bar_size is null but pkl path contains a TF component (e.g. .../1H/FOO.pkl),
    smoke_test_artifact must select the matching FOO_1H.parquet, NOT the first (1D) parquet."""

    raw_dir = tmp_path / "raw"
    stock_dir = raw_dir / "Stock_parquet"
    stock_dir.mkdir(parents=True, exist_ok=True)

    import pandas as pd

    # Create a 1D parquet and a 1H parquet.  Without the fix, the 1D one was always used.
    df_1d = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=120, freq="D", tz="UTC"),
            "open": 2.0,
            "high": 2.0,
            "low": 2.0,
            "close": 2.0,
            "volume": 10.0,
        }
    )
    (stock_dir / "BAR_1D.parquet").to_parquet if False else df_1d.to_parquet(stock_dir / "BAR_1D.parquet")

    df_1h = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=120, freq="h", tz="UTC"),
            "open": 3.0,
            "high": 3.0,
            "low": 3.0,
            "close": 3.0,
            "volume": 5.0,
        }
    )
    df_1h.to_parquet(stock_dir / "BAR_1H.parquet")

    # The artifact has bar_size=null (as serialised by early training code).
    artifact = {
        "schema_version": 1,
        "asset": {"symbol": "BAR", "asset_class": "stock", "bar_size": None},
        "feature_spec": {"features": ["close"], "feature_config": {"feature_settings": {}}},
        "safe_inference": DummyInfer(),
    }

    from octa_training.core.artifact_io import _compute_sha256_bytes

    pkl_bytes = pickle.dumps(artifact, protocol=4)
    # Place pkl at a path that encodes the timeframe: .../BAR/equities/1H/BAR.pkl
    pkl_dir = tmp_path / "BAR" / "equities" / "1H"
    pkl_dir.mkdir(parents=True, exist_ok=True)
    pkl_path = pkl_dir / "BAR.pkl"
    sha_path = pkl_dir / "BAR.sha256"
    pkl_path.write_bytes(pkl_bytes)
    sha_path.write_text(_compute_sha256_bytes(pkl_bytes), encoding="utf-8")

    out = smoke_test_artifact(str(pkl_path), str(raw_dir), last_n=5)
    assert out.get("symbol") == "BAR"


@pytest.mark.parametrize("bar_size", [None, ""])  # type: ignore[arg-type]
def test_smoke_test_keeps_strict_error_without_bar_size(tmp_path: Path, bar_size) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    artifact = {
        "schema_version": 1,
        "asset": {"symbol": "FOO", "asset_class": "stock", "bar_size": bar_size},
        "feature_spec": {"features": ["close"], "feature_config": {"feature_settings": {}}},
        "safe_inference": DummyInfer(),
    }

    pkl_bytes = pickle.dumps(artifact, protocol=4)
    pkl_path = tmp_path / "FOO.pkl"
    sha_path = tmp_path / "FOO.sha256"
    pkl_path.write_bytes(pkl_bytes)
    sha_path.write_text(_compute_sha256_bytes(pkl_bytes), encoding="utf-8")

    with pytest.raises(FileNotFoundError):
        smoke_test_artifact(str(pkl_path), str(raw_dir), last_n=5)
