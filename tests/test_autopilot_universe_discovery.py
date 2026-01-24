from __future__ import annotations

from pathlib import Path

import pandas as pd

from octa_ops.autopilot.universe import discover_universe


def test_universe_discovery_from_stock_parquet(tmp_path: Path):
    stock = tmp_path / "Stock_parquet"
    stock.mkdir(parents=True)
    p = stock / "AAA_1D.parquet"
    df = pd.DataFrame({"timestamp": pd.date_range("2024-01-01", periods=5, freq="D", tz="UTC"), "close": 1.0})
    df.to_parquet(p, index=False)

    u = discover_universe(stock_dir=str(stock), fx_dir=str(tmp_path / "FX_parquet"), crypto_dir=str(tmp_path / "Crypto_parquet"), futures_dir=str(tmp_path / "Future_parquet"), asset_map_path=str(tmp_path / "asset_map.yaml"))
    assert any(x.symbol == "AAA" for x in u)
    rec = next(x for x in u if x.symbol == "AAA")
    assert rec.parquet_paths and "1D" in rec.parquet_paths
