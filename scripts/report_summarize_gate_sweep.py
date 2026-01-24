#!/usr/bin/env python3
"""Summarize a gate sweep report.

Purpose:
- Identify PASS candidates (symbols + key gate details).
- Provide a compact statistical breakdown of dominant fail clusters.

This does NOT modify any gates or thresholds.

Usage:
  python3 scripts/report_summarize_gate_sweep.py --report reports/<sweep>.json

Outputs (next to the report by default):
- <report>_insights.json
- <report>_insights.txt
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


def _load_report(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text())


def _result_gate(result: Dict[str, Any]) -> Dict[str, Any]:
    g = result.get("gate")
    return g if isinstance(g, dict) else {}


def _cluster_key(reason: str) -> str:
    r = str(reason)
    if r.startswith("folds pass ratio too low"):
        return "folds_pass_ratio_too_low"
    if r.startswith("subwindow_stability_failed"):
        return "subwindow_stability_failed"
    if r.startswith("tail_kill_switch"):
        return "tail_kill_switch"
    if r.startswith("cost_stress_failed"):
        return "cost_stress_failed"
    if r.startswith("is_oos_degradation"):
        return "is_oos_degradation"
    # Default: prefix up to ':' or first 40 chars.
    if ":" in r:
        return r.split(":", 1)[0].strip()
    return r[:40].strip() if r else "unknown"


_ratio_re = re.compile(r"folds pass ratio too low:\s*([0-9.]+)\s*<")


def _extract_fold_ratio(reason: str) -> float | None:
    m = _ratio_re.search(str(reason))
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, help="Path to sweep report JSON")
    ap.add_argument("--out-dir", default=None, help="Output dir (default: same dir as report)")
    args = ap.parse_args()

    report_path = Path(args.report)
    if not report_path.exists():
        raise SystemExit(f"Report not found: {report_path}")

    out_dir = Path(args.out_dir) if args.out_dir else report_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    rep = _load_report(report_path)
    results = rep.get("results") or []

    passed = [r for r in results if r.get("passed") is True and not r.get("error")]
    failed = [r for r in results if r.get("passed") is False and not r.get("error")]
    errored = [r for r in results if r.get("error")]

    # PASS details
    pass_details: List[Dict[str, Any]] = []
    for r in passed:
        g = _result_gate(r)
        pass_details.append(
            {
                "symbol": r.get("symbol"),
                "run_id": r.get("run_id"),
                "gate_version": g.get("gate_version"),
                "passed_checks": g.get("passed_checks"),
                "metrics": r.get("metrics"),
            }
        )

    # Fail cluster stats
    reason_counts = Counter()
    cluster_counts = Counter()
    co_occurrence = Counter()
    ratio_bucket = Counter()

    for r in failed:
        g = _result_gate(r)
        reasons = g.get("reasons") if isinstance(g.get("reasons"), list) else []
        reasons_s = [str(x) for x in reasons if x]

        clusters = {_cluster_key(x) for x in reasons_s}
        for x in reasons_s:
            reason_counts[x] += 1
            cluster_counts[_cluster_key(x)] += 1
            fr = _extract_fold_ratio(x)
            if fr is not None:
                ratio_bucket[f"{fr:.2f}"] += 1

        # track top-2 clusters co-occurrence explicitly
        if "folds_pass_ratio_too_low" in clusters and "subwindow_stability_failed" in clusters:
            co_occurrence["folds_pass_ratio_too_low AND subwindow_stability_failed"] += 1

    payload = {
        "source_report": str(report_path),
        "created": rep.get("created_utc"),
        "run_id": rep.get("run_id"),
        "summary": {
            "n_total": len(results),
            "n_pass": len(passed),
            "n_fail": len(failed),
            "n_err": len(errored),
        },
        "pass_candidates": pass_details,
        "fail_clusters": {
            "cluster_counts": dict(cluster_counts.most_common(50)),
            "top_reasons": reason_counts.most_common(25),
            "fold_ratio_bucket_counts": dict(ratio_bucket),
            "co_occurrence": dict(co_occurrence),
        },
    }

    base = report_path.stem
    out_json = out_dir / f"{base}_insights.json"
    out_txt = out_dir / f"{base}_insights.txt"

    out_json.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")

    lines: List[str] = []
    lines.append(f"report: {report_path}")
    lines.append(f"run_id: {rep.get('run_id')}")
    lines.append(f"n_total: {len(results)}")
    lines.append(f"PASS: {len(passed)}")
    lines.append(f"FAIL: {len(failed)}")
    lines.append(f"ERR: {len(errored)}")
    lines.append("")

    lines.append("PASS candidates:")
    if not pass_details:
        lines.append("  (none)")
    for p in pass_details:
        lines.append(f"- {p.get('symbol')} gate={p.get('gate_version')}")
        pc = p.get("passed_checks")
        if isinstance(pc, list) and pc:
            lines.append(f"  passed_checks: {', '.join(str(x) for x in pc)}")

    lines.append("")
    lines.append("Top fail clusters:")
    for k, v in cluster_counts.most_common(15):
        lines.append(f"  {v:4d}  {k}")

    if ratio_bucket:
        lines.append("")
        lines.append("Fold pass ratio buckets (from reasons):")
        for k, v in sorted(ratio_bucket.items(), key=lambda kv: float(kv[0])):
            lines.append(f"  {v:4d}  {k}")

    if co_occurrence:
        lines.append("")
        lines.append("Co-occurrence:")
        for k, v in co_occurrence.most_common():
            lines.append(f"  {v:4d}  {k}")

    out_txt.write_text("\n".join(lines) + "\n")

    print(f"Wrote: {out_json}")
    print(f"Wrote: {out_txt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
