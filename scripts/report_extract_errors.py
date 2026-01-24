#!/usr/bin/env python3
"""Extract error symbols from a sweep report JSON.

Purpose:
- Turn sweep "error" entries into an explicit, reviewable list.
- Bucketize errors into a few actionable categories (data sanity vs. fold insufficiency).

This does NOT change any performance gates.

Usage:
  python3 scripts/report_extract_errors.py \
    --report reports/index_gate_sweep_1d_....json

Outputs (by default next to the report):
- <report>_errors.json
- <report>_errors.txt
- <report>_exclude_symbols.txt
- <report>_review_allow_negative_prices.txt
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _bucket_error(first_line: str) -> str:
    s = (first_line or "").strip()
    if not s:
        return "unknown"

    if "non-positive prices" in s or "contains non-positive prices" in s:
        return "data_sanity:non_positive_prices"
    if "high < max(open,close)" in s:
        return "data_sanity:ohlc_inconsistent"
    if "close column NaN fraction" in s:
        return "data_sanity:too_many_nans"
    if "missing required 'close'" in s or "missing required \"close\"" in s:
        return "data_sanity:missing_close"
    if "missing time column" in s:
        return "data_sanity:missing_time"

    if "Insufficient effective folds" in s:
        return "cv:insufficient_folds"

    return "other"


def _parse_report(path: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rep = json.loads(path.read_text())
    items = rep.get("results")
    if not isinstance(items, list):
        raise ValueError("report JSON missing 'results' list")

    out: List[Dict[str, Any]] = []
    for r in items:
        if not isinstance(r, dict):
            continue
        err = r.get("error")
        if not err:
            continue
        first = str(err).splitlines()[0].strip()
        out.append(
            {
                "symbol": r.get("symbol"),
                "run_id": r.get("run_id"),
                "error_first_line": first,
                "error_bucket": _bucket_error(first),
                "error": err,
            }
        )
    return rep, out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, help="Path to sweep report JSON")
    ap.add_argument("--out-dir", default=None, help="Output dir (default: same dir as report)")
    ap.add_argument("--max-full-errors", type=int, default=0, help="Include full error text for first N entries (0 = include all)")
    args = ap.parse_args()

    report_path = Path(args.report)
    if not report_path.exists():
        raise SystemExit(f"Report not found: {report_path}")

    out_dir = Path(args.out_dir) if args.out_dir else report_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    rep, errors = _parse_report(report_path)

    # Optionally trim full error payloads to keep JSON small.
    if args.max_full_errors and args.max_full_errors > 0:
        for i, e in enumerate(errors):
            if i >= args.max_full_errors:
                e["error"] = None

    bucket_counts = Counter(e["error_bucket"] for e in errors)

    # Default exclusion list: any symbol with an error should be excluded from
    # performance distribution summaries unless manually repaired.
    exclude_symbols = sorted({str(e.get("symbol")) for e in errors if e.get("symbol")})

    # Candidates for allow_negative_prices review (DO NOT auto-enable):
    allow_neg_candidates = sorted(
        {
            str(e.get("symbol"))
            for e in errors
            if e.get("symbol") and e.get("error_bucket") == "data_sanity:non_positive_prices"
        }
    )

    base = report_path.stem

    out_json = out_dir / f"{base}_errors.json"
    out_txt = out_dir / f"{base}_errors.txt"
    out_excl = out_dir / f"{base}_exclude_symbols.txt"
    out_review = out_dir / f"{base}_review_allow_negative_prices.txt"

    payload = {
        "source_report": str(report_path),
        "created": rep.get("created_utc"),
        "run_id": rep.get("run_id"),
        "n_total": len(rep.get("results") or []),
        "n_errors": len(errors),
        "bucket_counts": dict(bucket_counts),
        "errors": errors,
        "exclude_symbols": exclude_symbols,
        "review_allow_negative_prices_candidates": allow_neg_candidates,
    }

    out_json.write_text(json.dumps(payload, indent=2, sort_keys=False))

    # Human-readable text.
    lines: List[str] = []
    lines.append(f"report: {report_path}")
    lines.append(f"n_errors: {len(errors)}")
    lines.append("bucket_counts:")
    for k, v in bucket_counts.most_common():
        lines.append(f"  {v:4d}  {k}")

    lines.append("")
    lines.append("errors:")
    for e in errors:
        lines.append(f"- {e.get('symbol')}: {e.get('error_bucket')} :: {e.get('error_first_line')}")

    out_txt.write_text("\n".join(lines) + "\n")
    out_excl.write_text("\n".join(exclude_symbols) + ("\n" if exclude_symbols else ""))

    review_lines = [
        "# Candidates that triggered non-positive price checks.",
        "# Policy: DO NOT auto-enable allow_negative_prices.",
        "# Review each symbol's data source semantics first:",
        "# - If it's a true price index: fix upstream data; keep strict checks.",
        "# - If it's a rate/spread/indicator mislabeled as OHLC: route to a non-price pipeline.",
        "",
    ]
    review_lines += allow_neg_candidates
    out_review.write_text("\n".join(review_lines) + ("\n" if allow_neg_candidates else ""))

    print(f"Wrote: {out_json}")
    print(f"Wrote: {out_txt}")
    print(f"Wrote: {out_excl}")
    print(f"Wrote: {out_review}")
    print(f"Errors: {len(errors)} / total {len(rep.get('results') or [])}")
    if bucket_counts:
        print("Top buckets:")
        for k, v in bucket_counts.most_common(10):
            print(f"  {v:4d}  {k}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
