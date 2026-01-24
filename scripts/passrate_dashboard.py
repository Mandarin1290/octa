#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

TARGETS = {
    "1D": (10.0, 30.0),
    "1H": (2.0, 10.0),
    "30m": (1.0, 5.0),
    "5m": (0.2, 2.0),
    "1m": (0.1, 1.0),
}


def _load_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def _fmt_pct(x: float) -> str:
    return f"{x:6.2f}%"


def _fmt_pct_opt(x: float | None) -> str:
    if x is None:
        return "  N/A "
    return _fmt_pct(x)


def _load_global_gate_counts(run_dir: Path) -> tuple[int, int] | None:
    """Return (total, pass) from global gate run summary if present."""
    p = run_dir / "global_gate_1d" / "run_summary.json"
    if not p.exists():
        return None
    try:
        j = _load_json(p)
        counts = j.get("counts") or {}
        total = int(counts.get("total"))
        passed = int(counts.get("PASS"))
        return total, passed
    except Exception:
        return None


def _stage_counts(run_dir: Path, st: dict) -> dict:
    tf = str(st.get("timeframe") or "")

    # Prefer explicit fields.
    n_input = st.get("n_input")
    n_pass = st.get("n_pass")
    n_missing = st.get("n_missing_parquet")

    # Special-case 1D stage when sourced from global gate.
    if tf == "1D" and str(st.get("source") or "") == "global_gate_1d":
        gg = _load_global_gate_counts(run_dir)
        if gg is not None:
            n_input, n_pass = gg

    # Backward-compatible fallback.
    if n_input is None:
        n_input = n_pass
    if n_missing is None:
        n_missing = 0

    n_input_i = int(n_input or 0)
    n_pass_i = int(n_pass or 0)
    n_missing_i = int(n_missing or 0)
    n_eligible_i = max(n_input_i - n_missing_i, 0)

    coverage_pct: float | None = None
    if n_input_i > 0:
        coverage_pct = 100.0 * (n_eligible_i / n_input_i)

    pass_pct: float | None = None
    if n_eligible_i > 0:
        pass_pct = 100.0 * (n_pass_i / n_eligible_i)

    return {
        "timeframe": tf,
        "n_input": n_input_i,
        "n_missing_parquet": n_missing_i,
        "n_eligible": n_eligible_i,
        "n_pass": n_pass_i,
        "coverage_pct": coverage_pct,
        "pass_pct": pass_pct,
    }


def _recommend_factor(observations: dict[str, float]) -> tuple[float, str]:
    """Heuristic recommendation for OCTA_GATE_OVERLAY_PATH quality-min factor.

    Factor semantics:
    - <1.0 relaxes quality MIN thresholds
    - >1.0 tightens quality MIN thresholds

    We keep this intentionally simple and fail-safe.
    """
    # Focus mainly on intraday stages; 1D is a coarse prefilter.
    focus = ["1H", "30m", "5m", "1m"]
    below = []
    above = []
    for tf in focus:
        if tf not in observations or tf not in TARGETS:
            continue
        lo, hi = TARGETS[tf]
        v = observations[tf]
        if v < lo:
            below.append((tf, v, lo))
        elif v > hi:
            above.append((tf, v, hi))

    if below:
        # If we're materially below the band, relax more.
        worst = min(below, key=lambda t: t[1] / max(t[2], 1e-9))
        tf, v, lo = worst
        if v < 0.5 * lo:
            return 0.75, f"{tf} PASS-Rate {v:.2f}% << Ziel {lo:.2f}% (stark zu streng)"
        return 0.85, f"{tf} PASS-Rate {v:.2f}% < Ziel {lo:.2f}% (zu streng)"

    if above:
        # If we're above the band, tighten slightly.
        worst = max(above, key=lambda t: t[1] / max(t[2], 1e-9))
        tf, v, hi = worst
        if v > 1.5 * hi:
            return 1.05, f"{tf} PASS-Rate {v:.2f}% >> Ziel {hi:.2f}% (zu locker)"
        return 0.95, f"{tf} PASS-Rate {v:.2f}% > Ziel {hi:.2f}% (leicht zu locker)"

    return 1.0, "PASS-Raten liegen innerhalb der Zielbänder (Overlay nicht nötig)"


def main() -> int:
    ap = argparse.ArgumentParser(description="OCTA Cascade PASS-Rate Dashboard")
    ap.add_argument("--run-dir", required=True, help="Path to reports/cascade/<RUN_ID>")
    ap.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    run_summary = run_dir / "run_summary.json"
    if not run_summary.exists():
        raise SystemExit(f"run_summary.json not found: {run_summary}")

    rs = _load_json(run_summary)
    stages = rs.get("stages") or []

    rows = []
    observed_pct: dict[str, float] = {}
    for st in stages:
        try:
            c = _stage_counts(run_dir, st)
        except Exception:
            continue

        tf = c["timeframe"]
        lo, hi = TARGETS.get(tf, (None, None))

        # Compare targets against PASS-rate among eligible symbols (excluding missing parquet).
        if c["pass_pct"] is not None:
            observed_pct[tf] = float(c["pass_pct"])

        rows.append(
            {
                **c,
                "target_low": lo,
                "target_high": hi,
            }
        )

    factor, reason = _recommend_factor(observed_pct)

    payload = {
        "run_dir": str(run_dir),
        "targets": TARGETS,
        "stages": rows,
        "overlay_recommendation": {
            "quality_min_factor": factor,
            "reason": reason,
            "env": {
                "OCTA_GATE_OVERLAY_PATH": "configs/gate_overlay_relax_quality.yaml",
            },
        },
    }

    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    print(f"Run: {run_dir}")
    print("\nTargets (PASS-Rate Bands):")
    for tf, (lo, hi) in TARGETS.items():
        print(f"  {tf:>3}: {lo:>5.2f}% .. {hi:>5.2f}%")

    print("\nObserved:")
    for r in rows:
        tf = r["timeframe"]
        lo, hi = r["target_low"], r["target_high"]
        tgt = "" if lo is None else f" (target {lo:.2f}%..{hi:.2f}%)"
        missing = int(r.get("n_missing_parquet") or 0)
        eligible = int(r.get("n_eligible") or 0)
        cov = r.get("coverage_pct")
        if missing > 0:
            cov_txt = _fmt_pct_opt(cov)
            print(
                f"  {tf:>3}: {r['n_pass']:>5}/{eligible:<5} = {_fmt_pct_opt(r['pass_pct'])}{tgt}"
                f" | coverage {cov_txt} (missing {missing}/{r['n_input']})"
            )
        else:
            print(f"  {tf:>3}: {r['n_pass']:>5}/{eligible:<5} = {_fmt_pct_opt(r['pass_pct'])}{tgt}")

    print("\nOverlay recommendation:")
    print(f"  quality_min_factor ≈ {factor:.2f}  ({reason})")
    print("  Apply with:")
    print("    export OCTA_GATE_OVERLAY_PATH=configs/gate_overlay_relax_quality.yaml")
    print("  Then edit the factor in that file if desired.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
