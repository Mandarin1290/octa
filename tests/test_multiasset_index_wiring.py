"""A2 regression: Index symbols with SYMBOL_full_1day.parquet naming are discoverable."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from octa_ops.autopilot.universe import discover_universe


def _write_parquet(p: Path) -> None:
    df = pd.DataFrame(
        {"timestamp": pd.date_range("2024-01-01", periods=5, freq="D", tz="UTC"), "close": 1.0}
    )
    df.to_parquet(p, index=False)


def test_discover_universe_includes_index_fullname(tmp_path: Path) -> None:
    """AEX_full_1day.parquet → symbol=AEX, tf=1D, asset_class=index."""
    idx_d = tmp_path / "Indices_parquet"
    idx_d.mkdir()
    _write_parquet(idx_d / "AEX_full_1day.parquet")

    u = discover_universe(
        stock_dir=str(tmp_path / "Stock"),
        fx_dir=str(tmp_path / "FX"),
        crypto_dir=str(tmp_path / "Crypto"),
        futures_dir=str(tmp_path / "Futures"),
        etf_dir=str(tmp_path / "ETF"),
        index_dir=str(idx_d),
        asset_map_path=str(tmp_path / "asset_map.yaml"),
    )
    symbols = {s.symbol: s for s in u}
    assert "AEX" in symbols, "AEX not found in universe"
    assert symbols["AEX"].asset_class == "index"
    assert "1D" in (symbols["AEX"].parquet_paths or {})


def test_discover_universe_index_multiple_timeframes(tmp_path: Path) -> None:
    """SYMBOL_full_1day + SYMBOL_full_30min both parsed for same symbol."""
    idx_d = tmp_path / "Indices_parquet"
    idx_d.mkdir()
    _write_parquet(idx_d / "SPX_full_1day.parquet")
    _write_parquet(idx_d / "SPX_full_30min.parquet")

    u = discover_universe(
        stock_dir=str(tmp_path / "Stock"),
        fx_dir=str(tmp_path / "FX"),
        crypto_dir=str(tmp_path / "Crypto"),
        futures_dir=str(tmp_path / "Futures"),
        etf_dir=str(tmp_path / "ETF"),
        index_dir=str(idx_d),
        asset_map_path=str(tmp_path / "asset_map.yaml"),
    )
    symbols = {s.symbol: s for s in u}
    assert "SPX" in symbols
    paths = symbols["SPX"].parquet_paths or {}
    assert "1D" in paths
    assert "30M" in paths


def test_discover_universe_missing_index_dir_graceful(tmp_path: Path) -> None:
    """Missing index_dir is handled gracefully — no exception, no index symbols."""
    u = discover_universe(
        stock_dir=str(tmp_path / "Stock"),
        fx_dir=str(tmp_path / "FX"),
        crypto_dir=str(tmp_path / "Crypto"),
        futures_dir=str(tmp_path / "Futures"),
        etf_dir=str(tmp_path / "ETF"),
        index_dir=str(tmp_path / "nonexistent_Indices"),
        asset_map_path=str(tmp_path / "asset_map.yaml"),
    )
    assert all(s.asset_class != "index" for s in u)
