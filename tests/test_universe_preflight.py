from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd

from octa.support.ops.universe_preflight import DEFAULT_REQUIRED_TFS, scan_inventory, write_outputs


def _write_parquet(path: Path, *, include_time_column: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    idx = pd.date_range("2024-01-01", periods=4, freq="D", tz="UTC")
    if include_time_column:
        df = pd.DataFrame({"timestamp": idx, "close": [1.0, 1.1, 1.2, 1.3]})
        df.to_parquet(path, index=False)
    else:
        df = pd.DataFrame({"close": [1.0, 1.1, 1.2, 1.3], "open": [1.0, 1.05, 1.1, 1.2]})
        df.to_parquet(path, index=False)


def _write_parquet_with_datetime_index(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    idx = pd.date_range("2024-01-01", periods=4, freq="D", tz="UTC")
    df = pd.DataFrame({"close": [1.0, 1.1, 1.2, 1.3]}, index=idx)
    df.to_parquet(path, index=True)


def test_preflight_outputs_and_parsing(tmp_path: Path) -> None:
    eq_root = tmp_path / "raw" / "equities"
    for tf in DEFAULT_REQUIRED_TFS:
        _write_parquet(eq_root / "AAA" / f"AAA_{tf}.parquet")
        _write_parquet(eq_root / "BRK_B" / f"BRK_B_{tf}.parquet")

    _write_parquet(eq_root / "BBB" / "BBB_1D.parquet")
    _write_parquet(eq_root / "BBB" / "BBB_1H.parquet")
    _write_parquet(eq_root / "BBB" / "sub" / "BBB_30M.parquet")

    outdir = tmp_path / "out"
    result = scan_inventory(tmp_path / "raw", DEFAULT_REQUIRED_TFS, strict=True)
    paths = write_outputs(result, outdir)

    summary = json.loads(Path(paths["summary"]).read_text(encoding="utf-8"))
    trainable = Path(paths["trainable_symbols"]).read_text(encoding="utf-8").strip().splitlines()

    assert summary["trainable_count"] == 2
    assert trainable == ["AAA", "BRK_B"]

    excluded_lines = Path(paths["excluded"]).read_text(encoding="utf-8").splitlines()
    assert len(excluded_lines) == 1
    excluded = json.loads(excluded_lines[0])
    assert excluded["symbol"] == "BBB"
    assert excluded["missing_tfs"] == ["30M", "5M", "1M"]

    inventory_lines = Path(paths["inventory"]).read_text(encoding="utf-8").splitlines()
    assert len(inventory_lines) == 3

    # Deterministic output ordering
    assert trainable == sorted(trainable)


def test_preflight_excludes_symbol_for_missing_time_column_in_strict_mode(tmp_path: Path) -> None:
    eq_root = tmp_path / "raw" / "equities"
    for tf in DEFAULT_REQUIRED_TFS:
        _write_parquet(eq_root / "AAA" / f"AAA_{tf}.parquet")
        _write_parquet(eq_root / "BAD" / f"BAD_{tf}.parquet")

    _write_parquet(eq_root / "BAD" / "BAD_1D.parquet", include_time_column=False)

    outdir = tmp_path / "out_missing_time"
    result = scan_inventory(tmp_path / "raw", DEFAULT_REQUIRED_TFS, strict=True)
    paths = write_outputs(result, outdir)

    trainable = Path(paths["trainable_symbols"]).read_text(encoding="utf-8").strip().splitlines()
    assert trainable == ["AAA"]

    excluded_lines = Path(paths["excluded"]).read_text(encoding="utf-8").splitlines()
    assert len(excluded_lines) == 1
    excluded = json.loads(excluded_lines[0])
    assert excluded["symbol"] == "BAD"
    assert excluded["reason"] == "missing_time_column"
    invalid = excluded["invalid_time_axes"]
    assert len(invalid) == 1
    assert invalid[0]["timeframe"] == "1D"
    assert str(invalid[0]["offending_path"]).endswith("BAD_1D.parquet")


def test_preflight_accepts_datetime_index_without_time_column(tmp_path: Path) -> None:
    eq_root = tmp_path / "raw" / "equities"
    for tf in DEFAULT_REQUIRED_TFS:
        _write_parquet_with_datetime_index(eq_root / "IDX" / f"IDX_{tf}.parquet")

    outdir = tmp_path / "out_datetime_index"
    result = scan_inventory(tmp_path / "raw", DEFAULT_REQUIRED_TFS, strict=True)
    paths = write_outputs(result, outdir)

    trainable = Path(paths["trainable_symbols"]).read_text(encoding="utf-8").strip().splitlines()
    assert trainable == ["IDX"]
    excluded_lines = Path(paths["excluded"]).read_text(encoding="utf-8").splitlines()
    assert excluded_lines == []


def _write_symbol_tree(root: Path, asset_class: str, symbol: str) -> None:
    for tf in DEFAULT_REQUIRED_TFS:
        _write_parquet(root / "raw" / asset_class / symbol / f"{symbol}_{tf}.parquet")


def test_preflight_follow_symlinks_discovers_assets(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    _write_symbol_tree(source_root, "equities", "AAA")

    scan_root = tmp_path / "scan_root"
    scan_root.mkdir(parents=True, exist_ok=True)
    os.symlink(source_root / "raw" / "equities", scan_root / "equities")

    result_without = scan_inventory(scan_root, DEFAULT_REQUIRED_TFS, strict=True, follow_symlinks=False)
    paths_without = write_outputs(result_without, tmp_path / "out_without_symlink")
    summary_without = json.loads(Path(paths_without["summary"]).read_text(encoding="utf-8"))
    assert summary_without["total_symbols"] == 0
    assert summary_without["follow_symlinks"] is False

    result_with = scan_inventory(scan_root, DEFAULT_REQUIRED_TFS, strict=True, follow_symlinks=True)
    paths_with = write_outputs(result_with, tmp_path / "out_with_symlink")
    summary_with = json.loads(Path(paths_with["summary"]).read_text(encoding="utf-8"))
    trainable_with = Path(paths_with["trainable_symbols"]).read_text(encoding="utf-8").splitlines()
    assert summary_with["total_symbols"] == 1
    assert summary_with["follow_symlinks"] is True
    assert trainable_with == ["AAA"]


def test_inventory_includes_asset_class(tmp_path: Path) -> None:
    _write_symbol_tree(tmp_path, "equities", "AAA")
    _write_symbol_tree(tmp_path, "futures", "ES")

    result = scan_inventory(tmp_path / "raw", DEFAULT_REQUIRED_TFS, strict=True)
    paths = write_outputs(result, tmp_path / "out_asset_class")

    rows = [json.loads(line) for line in Path(paths["inventory"]).read_text(encoding="utf-8").splitlines() if line.strip()]
    by_symbol = {str(r["symbol"]): r for r in rows}
    assert by_symbol["AAA"]["asset_class"] == "equities"
    assert by_symbol["ES"]["asset_class"] == "futures"


def test_mixed_asset_class_excluded(tmp_path: Path) -> None:
    _write_symbol_tree(tmp_path, "equities", "MIX")
    _write_symbol_tree(tmp_path, "futures", "MIX")

    result = scan_inventory(tmp_path / "raw", DEFAULT_REQUIRED_TFS, strict=True)
    paths = write_outputs(result, tmp_path / "out_mixed_asset")

    trainable = Path(paths["trainable_symbols"]).read_text(encoding="utf-8").splitlines()
    assert "MIX" not in trainable
    excluded_rows = [json.loads(line) for line in Path(paths["excluded"]).read_text(encoding="utf-8").splitlines() if line.strip()]
    row = next(r for r in excluded_rows if r["symbol"] == "MIX")
    assert row["reason"] == "mixed_asset_class"
    assert sorted(row["asset_classes"]) == ["equities", "futures"]


def test_preflight_excludes_non_temporal_time_named_columns(tmp_path: Path) -> None:
    sym_root = tmp_path / "raw" / "futures" / "A6"
    for tf in DEFAULT_REQUIRED_TFS:
        path = sym_root / f"A6_{tf}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame({"timestamp": [0.47, 0.61, 0.82, 0.97], "close": [1.0, 1.1, 1.0, 1.2]})
        df.to_parquet(path, index=False)

    result = scan_inventory(tmp_path / "raw", DEFAULT_REQUIRED_TFS, strict=True)
    paths = write_outputs(result, tmp_path / "out_non_temporal")
    trainable = Path(paths["trainable_symbols"]).read_text(encoding="utf-8").splitlines()
    assert "A6" not in trainable
    excluded_rows = [json.loads(line) for line in Path(paths["excluded"]).read_text(encoding="utf-8").splitlines() if line.strip()]
    row = next(r for r in excluded_rows if r["symbol"] == "A6")
    assert row["reason"] == "missing_time_column"
