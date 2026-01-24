#!/usr/bin/env python3
"""Summarize a per-symbol training_30m run folder.

Writes:
- reports/training_30m/<run_id>/summary.json
- reports/gate_reports/<run_id>/gate_report.json
- reports/gate_reports/<run_id>/gate_report.csv

Primary signal source is cascade decision artifacts:
- reports/cascade/<run_id>/<SYMBOL>/<TF>/decision.json  (TF in {"30m","1H","1D"})

Logs are used only as a fallback when decision artifacts are missing.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

_TIMEFRAMES = ("30m", "1H", "1D")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _symbol_list(logs_dir: Path) -> list[str]:
    meta_path = logs_dir / "runner.meta.json"
    if meta_path.exists():
        meta = _load_json(meta_path)
        if isinstance(meta, dict) and isinstance(meta.get("symbols"), list):
            return [str(s).upper() for s in meta["symbols"]]

    # Fallback: infer from log filenames
    return sorted({p.stem.upper() for p in logs_dir.glob("*.log")})


def _find_deepest_decision(run_id: str, symbol: str) -> tuple[str | None, dict | None, Path | None]:
    # Prefer deepest (30m) if present; otherwise 1H; otherwise 1D.
    for tf in _TIMEFRAMES:
        decision_path = Path("reports/cascade") / run_id / symbol / tf / "decision.json"
        if decision_path.exists():
            payload = _load_json(decision_path)
            return tf, payload, decision_path
    return None, None, None


def _fallback_log_status(log_path: Path) -> tuple[str, list[str]]:
    # Conservative fallback: any traceback/error => error
    if not log_path.exists():
        return "pending", ["missing_log"]
    text = log_path.read_text(errors="ignore")
    lower = text.lower()
    if "training blocked by safety lock" in lower:
        return "error", ["safety_lock"]
    if "traceback" in lower:
        return "error", ["traceback"]
    if " error" in lower or "exception" in lower:
        return "error", ["error_in_log"]
    if not text.strip():
        return "pending", ["empty_log"]
    if "wrote multi-timeframe report" in lower:
        return "error", ["missing_decision_json"]
    return "pending", ["inconclusive_log"]


def summarize(run_id: str) -> dict:
    logs_dir = Path("reports/training_30m") / run_id
    if not logs_dir.exists():
        raise SystemExit(f"logs_dir not found: {logs_dir}")

    passed: list[str] = []
    failed: list[str] = []
    skipped: list[str] = []
    errored: list[str] = []
    pending: list[str] = []

    symbols = _symbol_list(logs_dir)
    per_symbol: dict[str, dict] = {}

    for symbol in symbols:
        tf, decision, decision_path = _find_deepest_decision(run_id, symbol)
        fail_reasons: list[str] = []
        status: str

        if isinstance(decision, dict) and isinstance(decision.get("status"), str):
            raw = str(decision["status"]).upper()
            fail_reasons = [str(r) for r in (decision.get("fail_reasons") or [])]
            if raw == "PASS":
                status = "passed"
                passed.append(symbol)
            elif raw == "FAIL":
                status = "failed"
                failed.append(symbol)
            elif raw.startswith("SKIP"):
                status = "skipped"
                skipped.append(symbol)
            elif raw == "ERR":
                status = "error"
                errored.append(symbol)
            else:
                status = "error"
                errored.append(symbol)
        else:
            # fallback to log
            log_path = logs_dir / f"{symbol}.log"
            status, fail_reasons = _fallback_log_status(log_path)
            if status == "passed":
                passed.append(symbol)
            elif status == "failed":
                failed.append(symbol)
            elif status == "pending":
                pending.append(symbol)
            else:
                errored.append(symbol)

        per_symbol[symbol] = {
            "symbol": symbol,
            "status": status,
            "decision_timeframe": tf,
            "decision_path": str(decision_path) if decision_path else None,
            "fail_reasons": fail_reasons,
            "log_path": str(logs_dir / f"{symbol}.log") if (logs_dir / f"{symbol}.log").exists() else None,
        }

    summary = {
        "run_id": run_id,
        "logs_dir": str(logs_dir),
        "created_utc": _utc_now(),
        "total": len(symbols),
        "passed": len(passed),
        "failed": len(failed),
        "skipped": len(skipped),
        "error": len(errored),
        "pending": len(pending),
        "passed_symbols": sorted(passed),
        "failed_symbols": sorted(failed),
        "skipped_symbols": sorted(skipped),
        "error_symbols": sorted(errored),
        "pending_symbols": sorted(pending),
    }

    # write run summary
    (logs_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    # gate_report (paths + artifacts) – minimal
    out_dir = Path("reports/gate_reports") / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    gate_report = {
        "run_id": run_id,
        "created_utc": summary["created_utc"],
        "total": summary["total"],
        "passed": summary["passed"],
        "failed": summary["failed"],
        "skipped": summary.get("skipped", 0),
        "error": summary["error"],
        "pending": summary.get("pending", 0),
        "symbols": {},
    }

    # Try to locate multitf reports for this run_id.
    reports_root = Path("reports")
    multitf_hits = {}
    for p in reports_root.glob(f"*_multitf_*{run_id}*.json"):
        sym = p.name.split("_multitf_", 1)[0].upper()
        multitf_hits.setdefault(sym, []).append(str(p))

    for sym in symbols:
        ent = per_symbol.get(sym, {"symbol": sym, "status": "error"})
        gate_report["symbols"][sym] = {
            **ent,
            "multitf_paths": sorted(multitf_hits.get(sym, [])),
        }

    (out_dir / "gate_report.json").write_text(json.dumps(gate_report, indent=2) + "\n")

    with (out_dir / "gate_report.csv").open("w", encoding="utf-8") as f:
        f.write("symbol,status,decision_timeframe,multitf_count,fail_reasons,log_path\n")
        for sym, ent in gate_report["symbols"].items():
            f.write(",".join([
                sym,
                str(ent.get("status") or ""),
                str(ent.get("decision_timeframe") or ""),
                str(len(ent.get("multitf_paths") or [])),
                " | ".join([str(x) for x in (ent.get("fail_reasons") or [])]).replace("\n", " "),
                str(ent.get("log_path") or ""),
            ]) + "\n")

    return summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    args = ap.parse_args()
    s = summarize(args.run_id)
    print(json.dumps({k: s[k] for k in ("run_id", "total", "passed", "failed", "error", "pending") if k in s}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
