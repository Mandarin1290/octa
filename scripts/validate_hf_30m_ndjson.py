#!/usr/bin/env python3
"""Validate HF 30m NDJSON invariants.

This validates the strict per-symbol semantics for the HF 30m reporting NDJSON:
- Exactly one record per symbol
- Required fields present
- Status in {PASS, FAIL_HF_METRICS, SKIP_30M_NOT_ELIGIBLE, ERROR}
- hf_metrics is present only when status in {PASS, FAIL_HF_METRICS}
- skip_reason required when status == SKIP_30M_NOT_ELIGIBLE

Usage:
  PYTHONPATH=. python scripts/validate_hf_30m_ndjson.py --run-id <run_id>
  PYTHONPATH=. python scripts/validate_hf_30m_ndjson.py --ndjson reports/training_30m/<run_id>/hf_30m.ndjson
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

ALLOWED_STATUSES = {"PASS", "FAIL_HF_METRICS", "SKIP_30M_NOT_ELIGIBLE", "ERROR"}


def _load_ndjson(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception as e:
                raise ValueError(f"L{ln}: invalid JSON: {e}") from e
            if not isinstance(rec, dict):
                raise ValueError(f"L{ln}: record is not an object")
            rec["_line"] = ln
            out.append(rec)
    return out


def _require(cond: bool, msg: str, errs: List[str]) -> None:
    if not cond:
        errs.append(msg)


def validate_records(recs: List[Dict[str, Any]]) -> Tuple[Counter, List[str]]:
    counts: Counter = Counter()
    errs: List[str] = []

    seen_symbol: Dict[str, int] = {}

    for rec in recs:
        ln = rec.get("_line")
        sym = rec.get("symbol")
        tf = rec.get("timeframe")
        status = rec.get("status")

        _require(isinstance(sym, str) and sym.strip(), f"L{ln}: missing/invalid symbol", errs)
        _require(isinstance(tf, str) and tf.strip(), f"L{ln}: missing/invalid timeframe", errs)
        _require(tf == "30m", f"L{ln}: timeframe must be '30m' (got {tf!r})", errs)
        _require(isinstance(status, str) and status in ALLOWED_STATUSES, f"L{ln}: invalid status {status!r}", errs)

        if isinstance(sym, str) and sym.strip():
            s = sym.strip().upper()
            if s in seen_symbol:
                errs.append(f"L{ln}: duplicate symbol record for {s} (first at L{seen_symbol[s]})")
            else:
                seen_symbol[s] = int(ln or 0)

        flags = rec.get("eligibility_flags")
        _require(isinstance(flags, dict), f"L{ln}: eligibility_flags missing/not a dict", errs)
        if isinstance(flags, dict):
            for k in ("daily_ok", "intraday_ok", "structure_ok"):
                _require(isinstance(flags.get(k), bool), f"L{ln}: eligibility_flags.{k} missing/not bool", errs)

        hf_metrics = rec.get("hf_metrics")
        if status in {"PASS", "FAIL_HF_METRICS"}:
            _require(isinstance(hf_metrics, dict), f"L{ln}: hf_metrics required for status={status}", errs)
        else:
            _require(hf_metrics is None, f"L{ln}: hf_metrics must be omitted for status={status}", errs)

        if status == "SKIP_30M_NOT_ELIGIBLE":
            _require(isinstance(rec.get("skip_reason"), str) and str(rec.get("skip_reason")).strip(), f"L{ln}: skip_reason required for SKIP", errs)
            _require(rec.get("fail_reasons") in (None, []), f"L{ln}: fail_reasons must be empty/omitted for SKIP", errs)

        if status == "FAIL_HF_METRICS":
            fr = rec.get("fail_reasons")
            _require(isinstance(fr, list) and len(fr) >= 1, f"L{ln}: fail_reasons list required for FAIL_HF_METRICS", errs)
            _require(rec.get("skip_reason") is None, f"L{ln}: skip_reason must be omitted for FAIL_HF_METRICS", errs)

        if status == "PASS":
            _require(rec.get("skip_reason") is None, f"L{ln}: skip_reason must be omitted for PASS", errs)
            _require(rec.get("fail_reasons") in (None, []), f"L{ln}: fail_reasons must be empty/omitted for PASS", errs)

        if status == "ERROR":
            # Allow fail_reasons for debugging but require at least one reason.
            fr = rec.get("fail_reasons")
            _require(isinstance(fr, list) and len(fr) >= 1, f"L{ln}: fail_reasons list required for ERROR", errs)

        if isinstance(status, str):
            counts[status] += 1

    counts["total"] = len(recs)
    counts["unique_symbols"] = len(seen_symbol)
    return counts, errs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default=None, help="Run id to resolve NDJSON at reports/training_30m/<run_id>/hf_30m.ndjson")
    ap.add_argument("--ndjson", default=None, help="Path to hf_30m.ndjson")
    args = ap.parse_args()

    ndjson_path: Path
    if args.ndjson:
        ndjson_path = Path(args.ndjson)
    elif args.run_id:
        ndjson_path = Path("reports") / "training_30m" / str(args.run_id) / "hf_30m.ndjson"
    else:
        raise SystemExit("Provide --run-id or --ndjson")

    if not ndjson_path.exists():
        print(f"NDJSON not found: {ndjson_path}")
        return 2

    try:
        recs = _load_ndjson(ndjson_path)
    except Exception as e:
        print(str(e))
        return 2

    counts, errs = validate_records(recs)
    if errs:
        for e in errs:
            print(e)
        print(json.dumps({"ndjson": str(ndjson_path), "counts": dict(counts)}, indent=2))
        return 2

    print("OK")
    print(json.dumps({"ndjson": str(ndjson_path), "counts": dict(counts)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
