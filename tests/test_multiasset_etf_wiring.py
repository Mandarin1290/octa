"""A1 regression: ETF symbols are discoverable via etf_dir param."""
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


def test_discover_universe_includes_etf(tmp_path: Path) -> None:
    """ETF parquet in etf_dir appears in universe with asset_class='etf'."""
    etf_d = tmp_path / "ETF_Parquet"
    etf_d.mkdir()
    _write_parquet(etf_d / "AADR_1D.parquet")

    u = discover_universe(
        stock_dir=str(tmp_path / "Stock"),
        fx_dir=str(tmp_path / "FX"),
        crypto_dir=str(tmp_path / "Crypto"),
        futures_dir=str(tmp_path / "Futures"),
        etf_dir=str(etf_d),
        asset_map_path=str(tmp_path / "asset_map.yaml"),
    )
    symbols = {s.symbol: s for s in u}
    assert "AADR" in symbols, "AADR not found in universe"
    assert symbols["AADR"].asset_class == "etf"
    assert "1D" in (symbols["AADR"].parquet_paths or {})


def test_discover_universe_etf_does_not_shadow_stock(tmp_path: Path) -> None:
    """If a symbol appears in both stock_dir and etf_dir, stock wins (first upsert)."""
    stock_d = tmp_path / "Stock_parquet"
    stock_d.mkdir()
    _write_parquet(stock_d / "DUAL_1D.parquet")

    etf_d = tmp_path / "ETF_Parquet"
    etf_d.mkdir()
    _write_parquet(etf_d / "DUAL_1D.parquet")

    u = discover_universe(
        stock_dir=str(stock_d),
        fx_dir=str(tmp_path / "FX"),
        crypto_dir=str(tmp_path / "Crypto"),
        futures_dir=str(tmp_path / "Futures"),
        etf_dir=str(etf_d),
        asset_map_path=str(tmp_path / "asset_map.yaml"),
    )
    symbols = {s.symbol: s for s in u}
    assert "DUAL" in symbols
    # Stock was inserted first → asset_class preserved as "equity"
    assert symbols["DUAL"].asset_class == "equity"


def test_discover_universe_missing_etf_dir_returns_gracefully(tmp_path: Path) -> None:
    """Missing etf_dir is handled gracefully — no exception, no ETF symbols."""
    u = discover_universe(
        stock_dir=str(tmp_path / "Stock"),
        fx_dir=str(tmp_path / "FX"),
        crypto_dir=str(tmp_path / "Crypto"),
        futures_dir=str(tmp_path / "Futures"),
        etf_dir=str(tmp_path / "nonexistent_ETF"),
        asset_map_path=str(tmp_path / "asset_map.yaml"),
    )
    assert all(s.asset_class != "etf" for s in u)
