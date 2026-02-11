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


def test_detect_asset_folders_case_insensitive_and_options_skipped(tmp_path: Path) -> None:
    (tmp_path / "stock_PARQUET").mkdir()
    (tmp_path / "Etf_parquet").mkdir()
    (tmp_path / "FX_Parquet").mkdir()
    (tmp_path / "futures_parquet").mkdir()
    (tmp_path / "CRYPTO_parquet").mkdir()
    (tmp_path / "Options_parquet").mkdir()
    out = detect_asset_folders(tmp_path, ignore_options=True)
    assert set(out.keys()) == {"equities", "etfs", "fx", "futures", "crypto"}
    assert all("option" not in p.name.lower() for p in out.values())


def test_completeness_gating_and_options_ignored_and_deterministic_manifest(tmp_path: Path) -> None:
    src = tmp_path / "src"
    dst = tmp_path / "raw"
    ev = tmp_path / "evidence"
    stock = src / "Stock_parquet"
    etf = src / "ETF_parquet"
    options = src / "Options_parquet"
    stock.mkdir(parents=True)
    etf.mkdir(parents=True)
    options.mkdir(parents=True)

    # Incomplete symbol: missing 1M -> must be skipped entirely.
    (stock / "AAPL_1D.parquet").write_bytes(b"a")
    (stock / "AAPL_1H.parquet").write_bytes(b"a")
    (stock / "AAPL_30M.parquet").write_bytes(b"a")
    (stock / "AAPL_5M.parquet").write_bytes(b"a")

    # Complete symbol: all required TFs -> must be linked.
    for tf in ("1D", "1H", "30M", "5M", "1M"):
        (stock / f"MSFT_{tf}.parquet").write_bytes(tf.encode("utf-8"))

    # Options-like filenames/folders -> ignored.
    (stock / "SPY_OPTIONS_1D.parquet").write_bytes(b"x")
    (stock / "QQQ_chain_1H.parquet").write_bytes(b"y")
    (options / "AAPL_1D.parquet").write_bytes(b"z")

    # Unparseable file -> error row in manifest.
    (etf / "bad_name.parquet").write_bytes(b"b")

    out = build_raw_tree(
        source_root=src,
        dest_root=dst,
        mode="symlink",
        dry_run=False,
        ignore_options=True,
        evidence_dir=ev,
    )

    plan = json.loads(Path(out["plan"]).read_text(encoding="utf-8"))
    comp = json.loads(Path(out["completeness_report"]).read_text(encoding="utf-8"))
    rows = [json.loads(x) for x in Path(out["manifest"]).read_text(encoding="utf-8").splitlines() if x.strip()]

    # completeness: AAPL skipped, MSFT eligible
    assert "MSFT" in comp["eligible_symbols"]["equities"]
    skipped = [x for x in comp["skipped_symbols"] if x["asset_class"] == "equities" and x["symbol"] == "AAPL"]
    assert len(skipped) == 1
    assert skipped[0]["missing_tfs"] == ["1M"]

    # no folder for incomplete AAPL
    assert not (dst / "equities" / "AAPL").exists()

    # complete MSFT folder with all required TFs
    msft_dir = dst / "equities" / "MSFT"
    assert msft_dir.exists()
    for tf in ("1D", "1H", "30M", "5M", "1M"):
        p = msft_dir / f"MSFT_{tf}.parquet"
        assert p.is_symlink()
        assert p.resolve() == (stock / f"MSFT_{tf}.parquet").resolve()

    # options-like files are not in plan actions
    plan_srcs = [Path(a["src"]).name for a in plan["actions"]]
    assert all("OPTION" not in s.upper() for s in plan_srcs)
    assert all("CHAIN" not in s.upper() for s in plan_srcs)

    # deterministic ordering in manifest: sorted by asset/symbol/timeframe + preface rows.
    ok_rows = [r for r in rows if r["status"] in {"OK", "EXISTS_MATCH", "DRY_RUN"}]
    key_rows = [(r["asset_class"], r["symbol"], r["timeframe"], Path(r["src"]).name) for r in ok_rows]
    assert key_rows == sorted(key_rows)

    # evidence artifacts
    assert Path(out["hashes"]).exists()
    summary = json.loads(Path(out["summary"]).read_text(encoding="utf-8"))
    assert summary["eligible_count"] == 1
    assert summary["skipped_incomplete_count"] >= 1
    assert summary["unparseable_count"] == 1
