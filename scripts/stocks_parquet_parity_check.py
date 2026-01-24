#!/usr/bin/env python3
"""Quick parity check for stock parquet timeframes.

Scans an output directory (default: raw/Stock_parquet) and reports:
- counts per timeframe (final .parquet only)
- missing symbols per timeframe relative to a baseline timeframe (default: 1D)

This is meant for monitoring long-running conversion fill-up runs.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def _symbols_for_tf(out_dir: Path, tf: str) -> set[str]:
    symbols: set[str] = set()
    for p in out_dir.glob(f"*_{tf}.parquet"):
        name = p.name
        # split at last underscore
        if not name.endswith(f"_{tf}.parquet"):
            continue
        sym = name[: -(len(tf) + len(".parquet") + 1)]
        if sym:
            symbols.add(sym)
    return symbols


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="raw/Stock_parquet")
    ap.add_argument("--timeframes", default="1D,1H,30M,5M,1M")
    ap.add_argument("--baseline", default="1D")
    ap.add_argument("--write-missing", action="store_true")
    ap.add_argument("--missing-dir", default="reports")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    if not out_dir.exists():
        raise SystemExit(f"missing out-dir: {out_dir}")

    tfs = [x.strip().upper() for x in str(args.timeframes).split(",") if x.strip()]
    baseline = str(args.baseline).strip().upper()
    if baseline not in tfs:
        tfs = [baseline] + [x for x in tfs if x != baseline]

    sym_by_tf = {tf: _symbols_for_tf(out_dir, tf) for tf in tfs}

    print("counts:")
    for tf in tfs:
        print(f"  {tf}: {len(sym_by_tf[tf])}")

    base_syms = sym_by_tf.get(baseline, set())
    print(f"baseline: {baseline} ({len(base_syms)})")

    missing_dir = Path(args.missing_dir)
    if args.write_missing:
        missing_dir.mkdir(parents=True, exist_ok=True)

    for tf in tfs:
        if tf == baseline:
            continue
        missing = sorted(base_syms - sym_by_tf[tf])
        extra = sorted(sym_by_tf[tf] - base_syms)
        print(f"diff {tf} vs {baseline}: missing={len(missing)} extra={len(extra)}")
        if args.write_missing:
            (missing_dir / f"stocks_missing_{tf}_vs_{baseline}.txt").write_text("\n".join(missing) + ("\n" if missing else ""))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
