#!/usr/bin/env python3
"""Aggregate 1H training gate FAIL reasons for a given passlist.

Goal: turn the "why does 1H pass=0" question into counts + examples.

This script is intentionally simple and uses the SQLite state registry as the
source of truth. It does *not* guarantee the stored result corresponds to 1H
unless the last run for a symbol was a 1H run.

Typical usage:
  PYTHONPATH=. python3 scripts/report_1h_fail_reasons.py \
    --passlist reports/e2e/pass_1d.txt \
    --state-db state/state.db \
    --out-dir reports/e2e
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class Row:
    symbol: str
    last_gate_result: Optional[str]
    last_train_time: Optional[str]
    last_fail_time: Optional[str]


def _read_symbols(path: Path) -> list[str]:
    symbols: list[str] = []
    for line in path.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        symbols.append(s)
    return symbols


def _fetch_row(conn: sqlite3.Connection, symbol: str) -> Optional[Row]:
    cur = conn.cursor()
    cur.execute(
        "SELECT symbol,last_gate_result,last_train_time,last_fail_time FROM symbol_state WHERE symbol = ?",
        (symbol,),
    )
    r = cur.fetchone()
    if not r:
        return None
    return Row(
        symbol=str(r[0]),
        last_gate_result=r[1],
        last_train_time=r[2],
        last_fail_time=r[3],
    )


def _iter_reasons(last_gate_result: Optional[str]) -> Iterable[str]:
    if not last_gate_result:
        return []
    s = str(last_gate_result)
    if not s.startswith("FAIL:"):
        return []
    payload = s[len("FAIL:") :]
    parts = [p.strip() for p in payload.split(";")]
    return [p for p in parts if p]


def _reason_key(reason: str) -> str:
    # Examples:
    # - "net_to_gross too low: 0.31 < 0.6" -> net_to_gross
    # - "cost_stress_failed sharpe=-4.26" -> cost_stress_failed
    # - "turnover_per_day too high: ..." -> turnover_per_day
    tok = (reason or "").strip().split()[0] if reason else ""
    return tok.rstrip(":")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--passlist", required=True, help="Path to passlist (one symbol per line)")
    ap.add_argument("--state-db", required=True, help="Path to state.db")
    ap.add_argument("--out-dir", required=True, help="Directory to write reports into")
    ap.add_argument("--top", type=int, default=25, help="How many top reasons to include")
    ap.add_argument("--examples", type=int, default=5, help="How many example symbols per reason")
    args = ap.parse_args()

    passlist = Path(args.passlist)
    state_db = Path(args.state_db)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    symbols = _read_symbols(passlist)

    conn = sqlite3.connect(str(state_db))
    try:
        rows: list[Row] = []
        missing_symbols: list[str] = []
        for sym in symbols:
            row = _fetch_row(conn, sym)
            if row is None:
                missing_symbols.append(sym)
                continue
            rows.append(row)
    finally:
        conn.close()

    status_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    examples: dict[str, list[str]] = defaultdict(list)

    rows_with_fail_reasons = 0
    for r in rows:
        lg = r.last_gate_result
        status = (str(lg).split(":", 1)[0] if lg else "MISSING")
        status_counts[status] += 1

        rs = list(_iter_reasons(lg))
        if rs:
            rows_with_fail_reasons += 1
        for reason in rs:
            key = _reason_key(reason)
            if not key:
                continue
            reason_counts[key] += 1
            if len(examples[key]) < int(args.examples):
                examples[key].append(r.symbol)

    top = []
    for key, cnt in reason_counts.most_common(int(args.top)):
        top.append({"reason": key, "count": int(cnt), "examples": examples.get(key, [])})

    summary = {
        "passlist": str(passlist),
        "state_db": str(state_db),
        "n_symbols": len(symbols),
        "n_with_state": len(rows),
        "n_missing_state": len(missing_symbols),
        "n_rows_with_fail_reasons": rows_with_fail_reasons,
        "status_counts": dict(status_counts),
        "top_reason_keys": top,
        "note": (
            "Reasons are derived from symbol_state.last_gate_result which only stores up to the first 3 gate reasons "
            "and may reflect the last run for the symbol (not guaranteed to be 1H)."
        ),
    }

    (out_dir / "fail_reasons_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))

    # Per-symbol table for debugging
    with (out_dir / "fail_reasons_rows.csv").open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["symbol", "last_gate_result", "last_train_time", "last_fail_time"],
        )
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "symbol": r.symbol,
                    "last_gate_result": r.last_gate_result or "",
                    "last_train_time": r.last_train_time or "",
                    "last_fail_time": r.last_fail_time or "",
                }
            )

    if missing_symbols:
        (out_dir / "missing_in_state.txt").write_text("\n".join(missing_symbols) + "\n")

    print(f"Wrote: {out_dir / 'fail_reasons_summary.json'}")
    print(f"Wrote: {out_dir / 'fail_reasons_rows.csv'}")
    if missing_symbols:
        print(f"Missing in state: {len(missing_symbols)} (see missing_in_state.txt)")

    # quick terminal summary
    print("Top reasons:")
    for item in top[: min(10, len(top))]:
        print(f"  {item['count']:>4}  {item['reason']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
