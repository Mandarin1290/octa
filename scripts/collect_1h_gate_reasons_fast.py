#!/usr/bin/env python3
"""Collect 1H gate failure reasons by running fast pipeline training.

Why this exists:
- The SQLite state DB often only stores `last_gate_result=FAIL` without reasons.
- For debugging "0 symbols pass 1H", we need an empirical histogram of reasons.

This script runs `octa_training.core.pipeline.train_evaluate_package` with `fast=True`
so it trains a reduced model set and stops early.

Example:
  PYTHONPATH=. python3 scripts/collect_1h_gate_reasons_fast.py \
    --passlist reports/e2e/pass_1d.txt \
    --config configs/e2e_real_raw_debug.yaml \
    --parquet-dir raw/Stock_parquet \
    --out-dir reports/e2e/1h_reason_report_fast \
    --limit 30
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

# Allow running this file directly: ensure repo root is on sys.path.
_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


def _read_symbols(path: Path) -> list[str]:
    syms: list[str] = []
    for line in path.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        syms.append(s)
    return syms


def _reason_key(reason: str) -> str:
    tok = (reason or "").strip().split()[0] if reason else ""
    return tok.rstrip(":")


def _parquet_for(symbol: str, parquet_dir: Path, timeframe: str) -> Path:
    # Current convention in repo: raw/Stock_parquet/{SYMBOL}_{TF}.parquet
    return parquet_dir / f"{symbol}_{timeframe}.parquet"


def _scan_raw_for_timeframe(raw_root: Path, timeframe: str) -> dict[str, Path]:
    out: dict[str, Path] = {}
    if not raw_root.exists():
        return out
    tf = str(timeframe)
    idx_bar = {
        "1D": "1day",
        "1H": "1hour",
        "30m": "30min",
        "5m": "5min",
        "1m": "1min",
    }.get(tf)
    suffixes = [f"_{tf}.parquet", f"_{tf.lower()}.parquet"]
    if idx_bar:
        suffixes.extend([
            f"_full_{idx_bar}.parquet",
            f"_full_{idx_bar.lower()}.parquet",
        ])
    # rglob per timeframe is cheaper than rglob per symbol.
    candidates: list[Path] = []
    for suf in suffixes:
        candidates.extend(raw_root.rglob(f"*{suf}"))
    for p in sorted(set(candidates)):
        name = p.name
        suf = next((s for s in suffixes if name.endswith(s)), "")
        if not suf:
            continue
        sym = name[: -len(suf)].strip()
        if not sym:
            continue
        out.setdefault(sym, p)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--passlist", required=True)
    ap.add_argument("--config", default=None, help="Training config YAML (optional)")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--parquet-dir", help="Directory containing *_{TF}.parquet files")
    src.add_argument("--raw-root", help="Scan recursively under this folder for *_{TF}.parquet")
    ap.add_argument("--timeframe", default="1H")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--limit", type=int, default=0, help="If >0, only process first N symbols")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--safe-mode", action="store_true", default=True)
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    from octa_training.core.config import load_config
    from octa_training.core.pipeline import train_evaluate_package
    from octa_training.core.state import StateRegistry

    passlist = Path(args.passlist)
    timeframe = str(args.timeframe)
    parquet_dir = Path(args.parquet_dir) if args.parquet_dir else None
    raw_root = Path(args.raw_root) if args.raw_root else None
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_index: dict[str, Path] = {}
    if raw_root is not None:
        parquet_index = _scan_raw_for_timeframe(raw_root, timeframe)

    cfg = load_config(args.config) if args.config else load_config()
    try:
        cfg.seed = int(args.seed)
    except Exception:
        pass

    state = StateRegistry(cfg.paths.state_dir)

    symbols = _read_symbols(passlist)
    if int(args.limit) > 0:
        symbols = symbols[: int(args.limit)]

    run_id = args.run_id or f"diag_{timeframe}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    status_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    examples: Dict[str, list[str]] = defaultdict(list)

    rows: list[dict[str, Any]] = []

    for i, sym in enumerate(symbols, start=1):
        if raw_root is not None:
            pq = parquet_index.get(sym)
            if pq is None:
                status_counts["MISSING_PARQUET"] += 1
                rows.append({"symbol": sym, "status": "MISSING_PARQUET", "parquet": "", "reasons": ""})
                continue
        else:
            assert parquet_dir is not None
            pq = _parquet_for(sym, parquet_dir, timeframe)
        if not pq.exists():
            status_counts["MISSING_PARQUET"] += 1
            rows.append({"symbol": sym, "status": "MISSING_PARQUET", "parquet": str(pq), "reasons": ""})
            continue

        try:
            res = train_evaluate_package(
                sym,
                cfg,
                state,
                run_id=f"{run_id}__{sym}",
                safe_mode=bool(args.safe_mode),
                smoke_test=False,
                parquet_path=str(pq),
                fast=True,
            )
            passed = bool(getattr(res, "passed", False))
            gate_obj = getattr(res, "gate_result", None)
            rr = getattr(gate_obj, "reasons", None) if gate_obj is not None else None
            reasons_list: list[str] = []
            if isinstance(rr, list):
                reasons_list = [str(x) for x in rr if x is not None]

            status = "PASS" if passed else "FAIL"
            status_counts[status] += 1

            for r in reasons_list:
                k = _reason_key(r)
                if not k:
                    continue
                reason_counts[k] += 1
                if len(examples[k]) < 5:
                    examples[k].append(sym)

            rows.append(
                {
                    "symbol": sym,
                    "status": status,
                    "parquet": str(pq),
                    "reasons": ";".join(reasons_list[:10]),
                }
            )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            status_counts["ERROR"] += 1
            rows.append({"symbol": sym, "status": "ERROR", "parquet": str(pq), "reasons": str(e)})

        if i % 10 == 0:
            print(f"Progress {i}/{len(symbols)}...")

    top = []
    for k, cnt in reason_counts.most_common(25):
        top.append({"reason": k, "count": int(cnt), "examples": examples.get(k, [])})

    summary = {
        "timeframe": str(args.timeframe),
        "n_symbols": len(symbols),
        "run_id_prefix": run_id,
        "status_counts": dict(status_counts),
        "top_reason_keys": top,
    }

    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))

    with (out_dir / "rows.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "status", "parquet", "reasons"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Wrote: {out_dir / 'summary.json'}")
    print("Top reasons:")
    for item in top[:10]:
        print(f"  {item['count']:>4}  {item['reason']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
