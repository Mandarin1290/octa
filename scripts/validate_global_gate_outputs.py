#!/usr/bin/env python3
"""Validate global gate NDJSON + summary invariants.

Usage:
  PYTHONPATH=. python scripts/validate_global_gate_outputs.py --jsonl reports/<run>.jsonl --summary reports/<run>.summary.json

Hard invariants:
- No record with gate.status in {PASS_FULL, PASS_LIMITED_STATISTICAL_CONFIDENCE} may have passed == false.
- No record whose reasons contain data_load_failed or missing_walk_forward may have PASS_* status.
- Summary JSON must include datasets[] with processed datasets.
- Sum of per-dataset status_counts equals selected.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

PASS_STATUSES = {"PASS_FULL", "PASS_LIMITED_STATISTICAL_CONFIDENCE"}


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_jsonl(jsonl_path: Path) -> Tuple[Dict[str, Counter], List[str]]:
    by_ds: Dict[str, Counter] = {}
    errors: List[str] = []

    with jsonl_path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            rec = json.loads(line)
            if rec.get("type") != "symbol":
                continue
            ds = rec.get("dataset") or "unknown"
            by_ds.setdefault(ds, Counter())

            gate = rec.get("gate") or {}
            status = gate.get("status")
            passed = bool(rec.get("passed"))
            reasons = gate.get("reasons") or []

            if status:
                by_ds[ds][f"status:{status}"] += 1

            if status in PASS_STATUSES and not passed:
                errors.append(f"L{ln}: PASS status but passed=false ({ds} {rec.get('symbol')} {status})")

            rtxt = "\n".join([str(r) for r in reasons])
            if ("data_load_failed" in rtxt or "missing_walk_forward" in rtxt) and status in PASS_STATUSES:
                errors.append(f"L{ln}: data/wf failure but PASS status ({ds} {rec.get('symbol')} {status})")

    return by_ds, errors


def validate_summary(summary_path: Path) -> List[str]:
    s = _load_json(summary_path)
    errs: List[str] = []

    datasets = s.get("datasets")
    if not isinstance(datasets, list) or len(datasets) == 0:
        errs.append("summary.datasets missing/empty")
        return errs

    for d in datasets:
        ds = d.get("dataset")
        selected = int(d.get("selected") or 0)
        status_counts = d.get("status_counts") or {}
        if not isinstance(status_counts, dict):
            errs.append(f"summary.datasets[{ds}].status_counts not a dict")
            continue
        total = 0
        for _, v in status_counts.items():
            try:
                total += int(v)
            except Exception:
                pass
        if selected and total != selected:
            errs.append(f"summary.datasets[{ds}] status_counts sum {total} != selected {selected}")

    return errs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", required=True)
    ap.add_argument("--summary", required=True)
    args = ap.parse_args()

    jsonl_path = Path(args.jsonl)
    summary_path = Path(args.summary)

    _, jsonl_errs = validate_jsonl(jsonl_path)
    summary_errs = validate_summary(summary_path)

    all_errs = jsonl_errs + summary_errs
    if all_errs:
        for e in all_errs:
            print(e)
        return 2

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
