from types import SimpleNamespace

import pytest

from octa.support.ops import run_institutional_train as rit


def test_institutional_train_skips_bad_parquet(monkeypatch):
    def _bad_load_parquet(_path):
        raise ValueError("contains non-positive prices")

    def _fake_universe(*_args, **_kwargs):
        return [
            SimpleNamespace(
                symbol="TEST",
                asset_class="stock",
                parquet_paths={"1D": "dummy.parquet"},
            )
        ]

    def _fake_cascade(*_args, **_kwargs):
        return {}, {}

    monkeypatch.setattr(rit, "load_parquet", _bad_load_parquet)
    monkeypatch.setattr(rit, "discover_universe", _fake_universe)
    monkeypatch.setattr(rit, "run_cascade_training", _fake_cascade)

    summary = rit.run_institutional_train(
        config_path="octa_training/config/training.yaml",
        universe_size=1,
        timeframes=["1D"],
        seed=42,
        bucket="default",
        parquet_root="raw",
        mode="paper",
    )

    assert summary.get("skips")
    record = summary["skips"][0]
    assert record["reason"] == "non_positive_prices"
    assert summary.get("skip_counts", {}).get("non_positive_prices") == 1
