from __future__ import annotations

from pathlib import Path

from octa_training.core.io_parquet import discover_parquets


def test_discover_parquets_recursive_symbol_folders_and_deterministic_order(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    (raw / "equities" / "AAPL").mkdir(parents=True)
    (raw / "equities" / "MSFT").mkdir(parents=True)
    (raw / "fx" / "EURUSD").mkdir(parents=True)

    p1 = raw / "equities" / "AAPL" / "AAPL_1D.parquet"
    p2 = raw / "equities" / "MSFT" / "MSFT_1H.parquet"
    p3 = raw / "fx" / "EURUSD" / "EURUSD_30M.parquet"
    p1.write_bytes(b"a")
    p2.write_bytes(b"b")
    p3.write_bytes(b"c")

    found = discover_parquets(raw)
    paths = [str(x.path) for x in found]
    symbols = [x.symbol for x in found]

    assert paths == sorted(paths, key=lambda x: x.upper())
    assert symbols == ["AAPL", "MSFT", "EURUSD"]

    found2 = discover_parquets(raw)
    assert [str(x.path) for x in found2] == paths
