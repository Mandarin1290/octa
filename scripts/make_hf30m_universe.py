#!/usr/bin/env python3
"""Build a real-data HF-30m candidate universe from existing stock parquet files.

Goal: get 30m to HF niveau without simulation/faking by selecting symbols that have
sufficient *real* history in raw/Stock_parquet and required timeframes present.

Outputs:
- a symbols text file (one symbol per line)
- a csv with basic stats + exclusion reasons

This does not run training; it only selects candidates.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq


@dataclass(frozen=True)
class Row:
    symbol: str
    daily_rows: int | None
    has_1d: bool
    has_1h: bool
    has_30m: bool
    include: bool
    reason: str


def _num_rows_parquet(path: Path) -> int:
    pf = pq.ParquetFile(path)
    md = pf.metadata
    if md is None:
        # very uncommon; fallback to scanning fragments
        return sum(pf.read_row_group(i).num_rows for i in range(pf.num_row_groups))
    return int(md.num_rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-stock-dir", default="raw/Stock_parquet", help="Directory containing *_1D.parquet etc")
    ap.add_argument("--min-daily-rows", type=int, default=600)
    ap.add_argument("--max-symbols", type=int, default=0, help="0 = no limit; otherwise keep top-N by daily rows")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    args = ap.parse_args()

    raw_dir = Path(args.raw_stock_dir)
    if not raw_dir.exists():
        raise SystemExit(f"raw stock dir not found: {raw_dir}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: list[Row] = []

    # Discover symbols from *_1D.parquet.
    for p in sorted(raw_dir.glob("*_1D.parquet")):
        symbol = p.name[: -len("_1D.parquet")].upper()
        p1d = raw_dir / f"{symbol}_1D.parquet"
        p1h = raw_dir / f"{symbol}_1H.parquet"
        p30 = raw_dir / f"{symbol}_30M.parquet"

        has_1d = p1d.exists()
        has_1h = p1h.exists()
        has_30m = p30.exists()

        daily_rows: int | None = None
        include = False
        reason = ""

        if not has_1d:
            reason = "missing_1D"
        elif not has_1h:
            reason = "missing_1H"
        elif not has_30m:
            reason = "missing_30M"
        else:
            try:
                daily_rows = _num_rows_parquet(p1d)
            except Exception as e:
                reason = f"read_error_1D:{type(e).__name__}"
            else:
                if daily_rows < int(args.min_daily_rows):
                    reason = f"insufficient_rows:{daily_rows}<{int(args.min_daily_rows)}"
                else:
                    include = True
                    reason = "ok"

        rows.append(
            Row(
                symbol=symbol,
                daily_rows=daily_rows,
                has_1d=has_1d,
                has_1h=has_1h,
                has_30m=has_30m,
                include=include,
                reason=reason,
            )
        )

    included = [r for r in rows if r.include]
    included.sort(key=lambda r: (r.daily_rows or 0), reverse=True)

    if args.max_symbols and args.max_symbols > 0:
        included = included[: int(args.max_symbols)]

    symbols_out = out_dir / "symbols.txt"
    symbols_out.write_text("\n".join([r.symbol for r in included]) + ("\n" if included else ""))

    csv_out = out_dir / "universe.csv"
    with csv_out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "include", "daily_rows", "has_1d", "has_1h", "has_30m", "reason"])
        for r in rows:
            w.writerow([r.symbol, int(r.include), r.daily_rows if r.daily_rows is not None else "", int(r.has_1d), int(r.has_1h), int(r.has_30m), r.reason])

    print(
        {
            "raw_stock_dir": str(raw_dir),
            "min_daily_rows": int(args.min_daily_rows),
            "total_seen": len(rows),
            "eligible": len([r for r in rows if r.include]),
            "written_symbols": len(included),
            "symbols_file": str(symbols_out),
            "csv": str(csv_out),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
