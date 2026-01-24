#!/usr/bin/env python3
"""Analyze FX-G0 (1D risk overlay) kill distribution.

Diagnostics-only script:
- Reads NDJSON records produced by scripts/global_gate_diagnose.py
- Filters dataset=='fx' and type=='symbol'
- Aggregates reasons starting with "fx_g0:"
- Buckets failures into risk-overlay categories
- Extracts metric values from gate.diagnostics (when present) and gate.robustness.details
- Prints a console report and writes a JSON summary under reports/

No gating thresholds are modified.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple

REPORT_GLOB = "fx_gate_1dRisk_1hAlpha_*.jsonl"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _find_latest_report(reports_dir: Path) -> Optional[Path]:
    cands = sorted(reports_dir.glob(REPORT_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None


def _iter_symbol_records(jsonl_path: Path) -> Iterable[Dict[str, Any]]:
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("type") != "symbol":
                continue
            if rec.get("dataset") != "fx":
                continue
            yield rec


def _get_gate(rec: Dict[str, Any]) -> Dict[str, Any]:
    g = rec.get("gate")
    return g if isinstance(g, dict) else {}


def _get_diagnostics_map(gate: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    diags = gate.get("diagnostics")
    if not isinstance(diags, list):
        return out
    for d in diags:
        if not isinstance(d, dict):
            continue
        name = d.get("name")
        if isinstance(name, str) and name:
            out[name] = d
    return out


def _get_fx_stage_results(gate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Added as additive diagnostic entry in the two-stage runner.
    diag_map = _get_diagnostics_map(gate)
    d = diag_map.get("fx_stage_results")
    if not isinstance(d, dict):
        return None
    v = d.get("value")
    return v if isinstance(v, dict) else None


@dataclass
class MetricValue:
    value: Optional[float]
    threshold: Optional[float]


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _diag_metric(gate: Dict[str, Any], name: str) -> MetricValue:
    dm = _get_diagnostics_map(gate)
    d = dm.get(name)
    if not d:
        return MetricValue(None, None)
    return MetricValue(_as_float(d.get("value")), _as_float(d.get("threshold")))


def _robust_cost_stress_sharpe(gate: Dict[str, Any]) -> Optional[float]:
    r = gate.get("robustness")
    if not isinstance(r, dict):
        return None
    details = r.get("details")
    if not isinstance(details, dict):
        return None
    cs = details.get("cost_stress")
    if not isinstance(cs, dict):
        return None
    return _as_float(cs.get("sharpe"))


def _robust_cost_stress_passed(gate: Dict[str, Any]) -> Optional[bool]:
    r = gate.get("robustness")
    if not isinstance(r, dict):
        return None
    details = r.get("details")
    if not isinstance(details, dict):
        return None
    cs = details.get("cost_stress")
    if not isinstance(cs, dict):
        return None
    v = cs.get("passed")
    return bool(v) if v is not None else None


def _g0_reasons(gate: Dict[str, Any]) -> List[str]:
    rr = gate.get("reasons")
    if not isinstance(rr, list):
        return []
    out = []
    for r in rr:
        if not r:
            continue
        s = str(r)
        if s.startswith("fx_g0:"):
            out.append(s)
    return out


BUCKETS = {
    "tail_kill_switch": re.compile(r"^fx_g0:tail_kill_switch:", re.IGNORECASE),
    "cost_stress_failed": re.compile(r"^fx_g0:cost_stress_failed\b", re.IGNORECASE),
    "turnover_per_day": re.compile(r"^fx_g0:turnover_per_day too high:", re.IGNORECASE),
    "net_to_gross": re.compile(r"^fx_g0:net_to_gross too low:", re.IGNORECASE),
}


def _bucket_for_reason(reason: str) -> str:
    for k, rx in BUCKETS.items():
        if rx.search(reason):
            return k
    return "other_g0"


def _stats(values: List[float]) -> Optional[Dict[str, float]]:
    vv = [float(x) for x in values if x is not None]
    vv = [x for x in vv if x == x]
    if not vv:
        return None
    vv_sorted = sorted(vv)
    n = len(vv_sorted)

    def pctl(p: float) -> float:
        if n == 1:
            return vv_sorted[0]
        # nearest-rank percentile
        k = int(round((p / 100.0) * (n - 1)))
        k = max(0, min(n - 1, k))
        return vv_sorted[k]

    return {
        "min": vv_sorted[0],
        "median": float(median(vv_sorted)),
        "p90": float(pctl(90.0)),
        "max": vv_sorted[-1],
        "n": float(n),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--jsonl",
        default="",
        help="Path to FX two-stage gate NDJSON (defaults to newest reports/fx_gate_1dRisk_1hAlpha_*.jsonl)",
    )
    ap.add_argument(
        "--out-json",
        default="",
        help="Write JSON summary to this path (default: reports/fx_g0_kill_distribution_<timestamp>.json)",
    )
    args = ap.parse_args()

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = Path(args.jsonl) if args.jsonl else (_find_latest_report(reports_dir) or Path(""))
    if not jsonl_path or not str(jsonl_path) or not jsonl_path.exists():
        raise SystemExit(f"No input jsonl found. Provide --jsonl or ensure reports/{REPORT_GLOB} exists.")

    out_json = Path(args.out_json) if args.out_json else (reports_dir / f"fx_g0_kill_distribution_{_now_tag()}.json")

    n_total = 0
    n_g0_die = 0
    n_reached_g1 = 0

    bucket_counts = Counter()
    bucket_symbols: Dict[str, List[str]] = {}

    # Per-bucket metric collections
    cvar_vals: List[float] = []
    turnover_vals: List[float] = []
    ntg_vals: List[float] = []
    cs_sharpes: List[float] = []

    # Closest-to-pass candidates (distance computed from diagnostic thresholds when available)
    cvar_excess: List[Tuple[float, str, float, float]] = []  # (excess, sym, value, thr)
    tpd_excess: List[Tuple[float, str, float, float]] = []
    ntg_shortfall: List[Tuple[float, str, float, float]] = []
    cs_closeness: List[Tuple[float, str]] = []  # we use |sharpe - threshold| if threshold known, else -sharpe ranking

    for rec in _iter_symbol_records(jsonl_path):
        n_total += 1
        sym = str(rec.get("symbol") or "")
        gate = _get_gate(rec)

        stage = _get_fx_stage_results(gate)
        g0_pass = None
        g1_seen = False
        if isinstance(stage, dict):
            g0_pass = bool((stage.get("g0_1d") or {}).get("passed", False))
            g1_seen = bool((stage.get("g1_1h") or {}).get("passed", False) or (stage.get("g1_1h") is not None))

        rr_g0 = _g0_reasons(gate)
        if g0_pass is False or (rr_g0 and not any(str(r).startswith("fx_g1:") for r in (gate.get("reasons") or []))):
            # Likely died in G0
            n_g0_die += 1
        else:
            # If we see any fx_g1:* or stage indicates g0 passed, it reached G1.
            reasons = gate.get("reasons") or []
            if any(str(r).startswith("fx_g1:") for r in reasons) or g0_pass is True or g1_seen:
                n_reached_g1 += 1

        # Bucket reasons (G0 only)
        for r in rr_g0:
            b = _bucket_for_reason(r)
            bucket_counts[b] += 1
            bucket_symbols.setdefault(b, []).append(sym)

        # Metrics (always from gate diagnostics/robustness; even if record is a G1 fail, this reflects the final gate.
        # For our current runs, G0-fails carry G0 diagnostics.)
        mv_cvar = _diag_metric(gate, "cvar_95_over_daily_vol")
        if mv_cvar.value is not None:
            cvar_vals.append(mv_cvar.value)
        if mv_cvar.value is not None and mv_cvar.threshold is not None and mv_cvar.value > mv_cvar.threshold:
            cvar_excess.append((mv_cvar.value - mv_cvar.threshold, sym, mv_cvar.value, mv_cvar.threshold))

        mv_tpd = _diag_metric(gate, "turnover_per_day")
        if mv_tpd.value is not None:
            turnover_vals.append(mv_tpd.value)
        if mv_tpd.value is not None and mv_tpd.threshold is not None and mv_tpd.value > mv_tpd.threshold:
            tpd_excess.append((mv_tpd.value - mv_tpd.threshold, sym, mv_tpd.value, mv_tpd.threshold))

        mv_ntg = _diag_metric(gate, "net_to_gross")
        if mv_ntg.value is not None:
            ntg_vals.append(mv_ntg.value)
        if mv_ntg.value is not None and mv_ntg.threshold is not None and mv_ntg.value < mv_ntg.threshold:
            ntg_shortfall.append((mv_ntg.threshold - mv_ntg.value, sym, mv_ntg.value, mv_ntg.threshold))

        cs_sh = _robust_cost_stress_sharpe(gate)
        if cs_sh is not None:
            cs_sharpes.append(cs_sh)
            # "closest" for failed cost stress: we don't know the exact configured threshold from the record,
            # so rank by highest sharpe (closest to passing for any monotone threshold).
            cs_passed = _robust_cost_stress_passed(gate)
            if cs_passed is False:
                cs_closeness.append((-cs_sh, sym))

    # Console output
    print(f"FX NDJSON: {jsonl_path}")
    print(f"Total FX symbols evaluated: {n_total}")
    print(f"Reached G1 (1H) vs died in G0 (1D): {n_reached_g1} vs {n_g0_die}")
    print("")

    print("G0 failure buckets (count of fx_g0 reasons):")
    for k in ["tail_kill_switch", "cost_stress_failed", "turnover_per_day", "net_to_gross", "other_g0"]:
        print(f"  - {k}: {int(bucket_counts.get(k, 0))}")
    print("")

    print("Metric value stats (all FX records where available):")
    s = _stats(cvar_vals)
    if s:
        print(f"  - cvar_95_over_daily_vol: n={int(s['n'])} min={s['min']:.6g} med={s['median']:.6g} p90={s['p90']:.6g} max={s['max']:.6g}")
    s = _stats(turnover_vals)
    if s:
        print(f"  - turnover_per_day: n={int(s['n'])} min={s['min']:.6g} med={s['median']:.6g} p90={s['p90']:.6g} max={s['max']:.6g}")
    s = _stats(ntg_vals)
    if s:
        print(f"  - net_to_gross: n={int(s['n'])} min={s['min']:.6g} med={s['median']:.6g} p90={s['p90']:.6g} max={s['max']:.6g}")
    s = _stats(cs_sharpes)
    if s:
        print(f"  - cost_stress_sharpe: n={int(s['n'])} min={s['min']:.6g} med={s['median']:.6g} p90={s['p90']:.6g} max={s['max']:.6g}")
    print("")

    def _top10(title: str, rows: List[Tuple], fmt):
        print(title)
        for row in rows[:10]:
            print("  - " + fmt(row))
        print("")

    cvar_excess.sort(key=lambda x: x[0])
    _top10(
        "Top 10 closest-to-pass by tail_kill_switch (smallest cvar exceedance):",
        cvar_excess,
        lambda r: f"{r[1]} excess={r[0]:.6g} (value={r[2]:.6g} thr={r[3]:.6g})",
    )

    tpd_excess.sort(key=lambda x: x[0])
    _top10(
        "Top 10 closest-to-pass by turnover_per_day (smallest exceedance):",
        tpd_excess,
        lambda r: f"{r[1]} excess={r[0]:.6g} (value={r[2]:.6g} thr={r[3]:.6g})",
    )

    ntg_shortfall.sort(key=lambda x: x[0])
    _top10(
        "Top 10 closest-to-pass by net_to_gross (smallest shortfall):",
        ntg_shortfall,
        lambda r: f"{r[1]} shortfall={r[0]:.6g} (value={r[2]:.6g} thr={r[3]:.6g})",
    )

    cs_closeness.sort(key=lambda x: x[0])
    _top10(
        "Top 10 closest-to-pass by cost_stress (highest cost_stress_sharpe among failed):",
        cs_closeness,
        lambda r: f"{r[1]} cost_stress_sharpe={-r[0]:.6g}",
    )

    # JSON summary
    summary = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "jsonl": str(jsonl_path),
        "fx_counts": {
            "total": n_total,
            "reached_g1": n_reached_g1,
            "died_in_g0": n_g0_die,
        },
        "g0_bucket_counts": dict(bucket_counts),
        "metric_stats": {
            "cvar_95_over_daily_vol": _stats(cvar_vals),
            "turnover_per_day": _stats(turnover_vals),
            "net_to_gross": _stats(ntg_vals),
            "cost_stress_sharpe": _stats(cs_sharpes),
        },
        "closest_to_pass": {
            "tail_kill_switch": [
                {"symbol": sym, "excess": exc, "value": v, "threshold": thr}
                for exc, sym, v, thr in cvar_excess[:10]
            ],
            "turnover_per_day": [
                {"symbol": sym, "excess": exc, "value": v, "threshold": thr}
                for exc, sym, v, thr in tpd_excess[:10]
            ],
            "net_to_gross": [
                {"symbol": sym, "shortfall": sh, "value": v, "threshold": thr}
                for sh, sym, v, thr in ntg_shortfall[:10]
            ],
            "cost_stress": [{"symbol": sym, "cost_stress_sharpe": -k} for k, sym in cs_closeness[:10]],
        },
    }

    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote JSON summary: {out_json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
