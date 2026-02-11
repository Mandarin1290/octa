from __future__ import annotations

import json
from pathlib import Path

from octa.support.ops.build_raw_tree_from_flat_parquets import (
    build_raw_tree,
    detect_asset_folders,
    parse_filename_strict,
)


def test_parse_filename_strict_accepts_and_rejects() -> None:
    ok = parse_filename_strict("AAPL_1D.parquet")
    assert ok is not None
    assert ok.symbol == "AAPL"
    assert ok.timeframe == "1D"
    assert parse_filename_strict("AAPL_2H.parquet") is None
    assert parse_filename_strict("aapl_1D.parquet") is None
    assert parse_filename_strict("AAPL_1D.csv") is None


def test_detect_asset_folders_case_insensitive(tmp_path: Path) -> None:
    (tmp_path / "stock_PARQUET").mkdir()
    (tmp_path / "Etf_parquet").mkdir()
    (tmp_path / "FX_Parquet").mkdir()
    (tmp_path / "futures_parquet").mkdir()
    (tmp_path / "CRYPTO_parquet").mkdir()
    out = detect_asset_folders(tmp_path)
    assert set(out.keys()) == {"equities", "etfs", "fx", "futures", "crypto"}


def test_build_tree_dry_run_and_real_symlink_is_deterministic(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "raw"
    ev_dry = tmp_path / "evidence_dry"
    ev_real = tmp_path / "evidence_real"
    stock = src / "Stock_parquet"
    etf = src / "ETF_parquet"
    stock.mkdir(parents=True)
    etf.mkdir(parents=True)

    (stock / "MSFT_1H.parquet").write_bytes(b"b")
    (stock / "AAPL_1D.parquet").write_bytes(b"a")
    (stock / "bad_name.parquet").write_bytes(b"x")
    (etf / "SPY_30M.parquet").write_bytes(b"c")

    out_dry = build_raw_tree(source_root=src, dest_root=dst, mode="symlink", dry_run=True, evidence_dir=ev_dry)
    man_dry = Path(out_dry["manifest"])
    rows_dry = [json.loads(x) for x in man_dry.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert [Path(r["src"]).name for r in rows_dry] == ["AAPL_1D.parquet", "MSFT_1H.parquet", "bad_name.parquet", "SPY_30M.parquet"]
    assert any(r["status"] == "ERROR" and r.get("error") == "invalid_filename_format" for r in rows_dry)
    assert not (dst / "equities" / "AAPL" / "AAPL_1D.parquet").exists()

    out_real = build_raw_tree(source_root=src, dest_root=dst, mode="symlink", dry_run=False, evidence_dir=ev_real)
    man_real = Path(out_real["manifest"])
    rows_real = [json.loads(x) for x in man_real.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert [Path(r["src"]).name for r in rows_real] == ["AAPL_1D.parquet", "MSFT_1H.parquet", "bad_name.parquet", "SPY_30M.parquet"]
    aapl_dst = dst / "equities" / "AAPL" / "AAPL_1D.parquet"
    msft_dst = dst / "equities" / "MSFT" / "MSFT_1H.parquet"
    spy_dst = dst / "etfs" / "SPY" / "SPY_30M.parquet"
    assert aapl_dst.is_symlink()
    assert msft_dst.is_symlink()
    assert spy_dst.is_symlink()
    assert aapl_dst.resolve() == (stock / "AAPL_1D.parquet").resolve()
    assert msft_dst.resolve() == (stock / "MSFT_1H.parquet").resolve()
    assert spy_dst.resolve() == (etf / "SPY_30M.parquet").resolve()

    summary = json.loads(Path(out_real["summary"]).read_text(encoding="utf-8"))
    assert summary["counts_by_asset_class"]["equities"] == 2
    assert summary["counts_by_asset_class"]["etfs"] == 1
    assert summary["error_count"] == 1
    assert summary["unique_symbols"] == 3
    assert (Path(out_real["hashes"])).exists()
