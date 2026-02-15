from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from octa.core.data.sources.altdata.orchestrator import build_altdata_stack
from octa_ops.autopilot.universe import resolve_parquet_for_symbol_tf


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]], columns: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(columns))
        w.writeheader()
        for row in rows:
            w.writerow({c: row.get(c) for c in columns})


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_symbols(raw: str) -> List[str]:
    vals = [str(x).strip().upper() for x in str(raw).split(",")]
    return [x for x in vals if x]


def _parse_tfs(raw: str) -> List[str]:
    vals = [str(x).strip().upper() for x in str(raw).split(",")]
    out = [x for x in vals if x]
    if not out:
        raise RuntimeError("required_tfs_empty")
    return out


def _local_raw_link_path(symbol: str, tf: str, raw_root: str) -> Path:
    return Path(raw_root) / "equities" / symbol.upper() / f"{symbol.upper()}_{tf.upper()}.parquet"


def _classify_symbol(
    *,
    symbol: str,
    required_tfs: Sequence[str],
    raw_root: str,
    min_nonzero_bytes: int,
) -> Tuple[str, List[Dict[str, Any]], str]:
    rows: List[Dict[str, Any]] = []
    all_ok = True
    reason = "OK"
    for tf in required_tfs:
        path, resolver_reason = resolve_parquet_for_symbol_tf(symbol=symbol, tf=tf, raw_root=raw_root)
        exists = bool(path) and Path(str(path)).exists()
        size = int(Path(str(path)).stat().st_size) if exists else 0
        local_link = _local_raw_link_path(symbol=symbol, tf=tf, raw_root=raw_root)
        broken_symlink = bool(local_link.is_symlink() and not local_link.resolve(strict=False).exists())
        tf_ok = bool(exists and size > int(min_nonzero_bytes))
        rows.append(
            {
                "symbol": symbol,
                "tf": tf,
                "resolved_path": path,
                "resolver_reason": resolver_reason,
                "exists": bool(exists),
                "size_bytes": size,
                "local_link_path": str(local_link),
                "local_link_is_symlink": bool(local_link.is_symlink()),
                "broken_symlink_target": bool(broken_symlink),
                "ok": tf_ok,
            }
        )
        if not tf_ok:
            all_ok = False
            if broken_symlink:
                reason = "SKIP_broken_symlink_target"
            elif exists and size <= int(min_nonzero_bytes):
                reason = "SKIP_zero_byte"
            else:
                reason = "SKIP_not_found"
    return ("OK" if all_ok else reason), rows, reason


def _scan_equities_candidates(raw_root: str) -> List[str]:
    equities = Path(raw_root) / "equities"
    if not equities.exists():
        return []
    syms = [p.name.upper() for p in equities.iterdir() if p.is_dir() and p.name.strip()]
    return sorted(set(syms))


def _build_universe(
    *,
    requested_symbols: Sequence[str],
    required_tfs: Sequence[str],
    raw_root: str,
    min_nonzero_bytes: int,
    min_symbols: int,
    max_symbols: int,
) -> Dict[str, Any]:
    requested_rows: List[Dict[str, Any]] = []
    resolver_rows: List[Dict[str, Any]] = []
    selected: List[str] = []
    reason_counts: Dict[str, int] = {}

    for sym in requested_symbols:
        status, rows, reason = _classify_symbol(
            symbol=sym,
            required_tfs=required_tfs,
            raw_root=raw_root,
            min_nonzero_bytes=min_nonzero_bytes,
        )
        requested_rows.append({"symbol": sym, "status": status})
        resolver_rows.extend(rows)
        reason_counts[status] = int(reason_counts.get(status, 0)) + 1
        if status == "OK":
            selected.append(sym)

    for sym in _scan_equities_candidates(raw_root=raw_root):
        if len(selected) >= int(max_symbols):
            break
        if sym in selected:
            continue
        status, rows, _ = _classify_symbol(
            symbol=sym,
            required_tfs=required_tfs,
            raw_root=raw_root,
            min_nonzero_bytes=min_nonzero_bytes,
        )
        resolver_rows.extend(rows)
        if status == "OK":
            selected.append(sym)
        if len(selected) >= int(min_symbols):
            # continue to max_symbols only if requested already exceeded min; keep bounded and deterministic
            continue

    selected = selected[: int(max_symbols)]
    symbols_rows = []
    for sym in selected:
        paths: Dict[str, str] = {}
        for tf in required_tfs:
            p, _ = resolve_parquet_for_symbol_tf(symbol=sym, tf=tf, raw_root=raw_root)
            if p:
                paths[tf] = str(p)
        symbols_rows.append({"symbol": sym, "asset_class": "equities", "paths": paths})

    return {
        "requested_rows": requested_rows,
        "resolver_rows": resolver_rows,
        "selected_symbols": selected,
        "symbol_rows": symbols_rows,
        "requested_reason_counts": reason_counts,
    }


def _write_hashes(out_dir: Path) -> None:
    rows: List[Dict[str, Any]] = []
    for p in sorted(x for x in out_dir.rglob("*") if x.is_file()):
        if p.name == "hashes.txt":
            continue
        rows.append({"path": str(p), "sha256": _sha256(p)})
    _write_json(out_dir / "hashes.json", {"files": rows, "generated_at_utc": _utc_iso()})
    with (out_dir / "hashes.txt").open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(f"{row['sha256']}  {row['path']}\n")


def _run_micro_profiles(
    *,
    out_dir: Path,
    micro_universe_path: Path,
    runtime_cap_seconds: int,
    profiles: Sequence[str],
    export_series_out: bool,
    export_on_timeout: bool,
    decision_trace_out: bool,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    profiles_dir = out_dir / "profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    for p in profiles:
        profile = str(p).strip().lower()
        overlay = {"profile": profile, "overrides": {}}
        overlay_path = profiles_dir / f"{profile}.json"
        _write_json(overlay_path, overlay)
        run_root = out_dir / "micro" / profile
        run_root.mkdir(parents=True, exist_ok=True)
        cmd = [
            sys.executable,
            "-m",
            "octa_ops.autopilot.run_micro_calibration",
            "--micro-universe",
            str(micro_universe_path),
            "--profile-overlay",
            str(overlay_path),
            "--profile-name",
            profile,
            "--out-root",
            str(run_root),
            "--run-max-seconds",
            str(int(runtime_cap_seconds)),
            "--symbol-max-seconds",
            str(max(60, int(runtime_cap_seconds) // 4)),
            "--max-files",
            "200",
        ]
        if export_series_out:
            cmd.extend(["--export-series-out", str(run_root / "series_export")])
        if export_on_timeout:
            cmd.append("--export-on-timeout")
        if decision_trace_out:
            cmd.extend(["--decision-trace-out", str(run_root / "decision_trace")])
        stdout = run_root / "stdout.txt"
        stderr = run_root / "stderr.txt"
        with stdout.open("w", encoding="utf-8") as so, stderr.open("w", encoding="utf-8") as se:
            rc = subprocess.run(cmd, stdout=so, stderr=se, check=False).returncode
        results[profile] = {"rc": int(rc), "run_root": str(run_root)}
    return results


def _run_paper_shadow(*, out_dir: Path, max_symbols: int) -> Dict[str, Any]:
    ev_dir = out_dir / "paper_shadow"
    ev_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "octa.execution.cli.run_paper_shadow",
        "--evidence-dir",
        str(ev_dir),
        "--max-symbols",
        str(int(max_symbols)),
    ]
    stdout = ev_dir / "stdout.txt"
    stderr = ev_dir / "stderr.txt"
    with stdout.open("w", encoding="utf-8") as so, stderr.open("w", encoding="utf-8") as se:
        rc = subprocess.run(cmd, stdout=so, stderr=se, check=False).returncode
    summary_path = ev_dir / "paper_shadow_session_summary.json"
    summary = _read_json(summary_path) if summary_path.exists() else {}
    return {"rc": int(rc), "evidence_dir": str(ev_dir), "session_summary_path": str(summary_path), "summary": summary}


def _run_altdata_probe(*, out_dir: Path, symbols: Sequence[str]) -> Dict[str, Any]:
    run_id = f"v000_local_only_altdata_{_utc_now()}"
    summary = build_altdata_stack(run_id=run_id, symbols=list(symbols), allow_net=False)
    src = summary.get("sources", {}) if isinstance(summary.get("sources"), dict) else {}
    counts = {"available": 0, "missing_ignored": 0, "error_fail_closed": 0, "other": 0}
    for v in src.values():
        st = str((v or {}).get("status", "other"))
        if st in counts:
            counts[st] += 1
        else:
            counts["other"] += 1
    payload = {"run_id": run_id, "summary": summary, "counts": counts}
    _write_json(out_dir / "altdata_probe_summary.json", payload)
    return payload


def run(args: argparse.Namespace) -> Dict[str, Any]:
    out_dir = Path(args.out) if args.out else Path("octa/var/evidence") / f"v000_finish_paper_ready_local_only_{_utc_now()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "RUN_ID.txt").write_text(out_dir.name + "\n", encoding="utf-8")
    (out_dir / "OUT_PATH.txt").write_text(str(out_dir) + "\n", encoding="utf-8")
    (out_dir / "timestamp_utc.txt").write_text(_utc_iso() + "\n", encoding="utf-8")
    subprocess.run(["git", "rev-parse", "HEAD"], check=False, stdout=(out_dir / "head.txt").open("w", encoding="utf-8"))
    subprocess.run(["git", "status", "--porcelain=v1"], check=False, stdout=(out_dir / "status_before.txt").open("w", encoding="utf-8"))
    subprocess.run(["git", "diff", "--stat"], check=False, stdout=(out_dir / "git_diff_stat_global_before.txt").open("w", encoding="utf-8"))

    requested_symbols = _parse_symbols(args.requested_symbols)
    required_tfs = _parse_tfs(args.required_tfs)
    universe = _build_universe(
        requested_symbols=requested_symbols,
        required_tfs=required_tfs,
        raw_root="raw",
        min_nonzero_bytes=int(args.min_nonzero_bytes),
        min_symbols=int(args.min_symbols),
        max_symbols=int(args.max_symbols),
    )

    _write_json(out_dir / "requested_symbol_status.json", {"rows": universe["requested_rows"]})
    _write_csv(out_dir / "requested_symbol_status.csv", universe["requested_rows"], ["symbol", "status"])
    _write_json(out_dir / "resolver_probe_table.json", {"rows": universe["resolver_rows"]})
    _write_csv(
        out_dir / "resolver_probe_table.csv",
        universe["resolver_rows"],
        ["symbol", "tf", "resolved_path", "resolver_reason", "exists", "size_bytes", "local_link_path", "local_link_is_symlink", "broken_symlink_target", "ok"],
    )

    selected_symbols = list(universe["selected_symbols"])
    universe_rows = list(universe["symbol_rows"])
    substituted = [s for s in selected_symbols if s not in set(requested_symbols)]
    paper_universe_payload = {
        "source": "v000_finish_paper_ready_local_only",
        "required_tfs": required_tfs,
        "requested_symbols": requested_symbols,
        "selected_symbols": selected_symbols,
        "substituted_symbols": substituted,
        "symbol_count": len(selected_symbols),
        "symbols": universe_rows,
        "global_end_anchor_hint": _utc_iso(),
    }
    _write_json(out_dir / "paper_universe.json", paper_universe_payload)
    _write_csv(out_dir / "paper_universe.csv", [{"symbol": s} for s in selected_symbols], ["symbol"])
    _write_json(
        out_dir / "universe_table.json",
        {"rows": [{"symbol": s, "requested": s in set(requested_symbols), "substituted": s in set(substituted)} for s in selected_symbols]},
    )
    _write_csv(
        out_dir / "universe_table.csv",
        [{"symbol": s, "requested": s in set(requested_symbols), "substituted": s in set(substituted)} for s in selected_symbols],
        ["symbol", "requested", "substituted"],
    )

    precondition_ok = len(selected_symbols) >= int(args.min_symbols)
    micro_results: Dict[str, Any] = {}
    paper_results: Dict[str, Any] = {}
    altdata_payload = _run_altdata_probe(out_dir=out_dir, symbols=selected_symbols)
    if precondition_ok:
        micro_results = _run_micro_profiles(
            out_dir=out_dir,
            micro_universe_path=out_dir / "paper_universe.json",
            runtime_cap_seconds=int(args.runtime_cap_seconds),
            profiles=[x.strip() for x in str(args.profiles).split(",") if x.strip()],
            export_series_out=bool(args.export_series_out),
            export_on_timeout=bool(args.export_on_timeout),
            decision_trace_out=bool(args.decision_trace_out),
        )
        paper_results = _run_paper_shadow(out_dir=out_dir, max_symbols=min(len(selected_symbols), int(args.max_symbols)))

    requested_ok = int(sum(1 for r in universe["requested_rows"] if r["status"] == "OK"))
    requested_skipped = int(len(requested_symbols) - requested_ok)
    micro_all_rc0 = bool(micro_results) and all(int(v.get("rc", 1)) == 0 for v in micro_results.values())
    paper_rc0 = bool(paper_results) and int(paper_results.get("rc", 1)) == 0
    altdata_ok = int((altdata_payload.get("counts") or {}).get("error_fail_closed", 0)) == 0
    readiness_pass = bool(precondition_ok and micro_all_rc0 and paper_rc0 and altdata_ok)
    summary = {
        "pass": readiness_pass,
        "precondition_ok": precondition_ok,
        "selected_universe_count": len(selected_symbols),
        "min_symbols": int(args.min_symbols),
        "requested_ok": requested_ok,
        "requested_skipped": requested_skipped,
        "substituted_count": len(substituted),
        "requested_reason_counts": universe["requested_reason_counts"],
        "altdata_counts": altdata_payload.get("counts"),
        "micro_results": micro_results,
        "paper_results": paper_results,
        "next_actions": (
            "Only way forward is: add more local data"
            if not precondition_ok
            else "Re-run with the same locked universe to verify determinism"
        ),
    }
    _write_json(out_dir / "v0_paper_ready.json", summary)
    lines = [
        "# v0.0.0 Paper-Ready Foundation (Local Only)",
        "",
        f"- PASS: {readiness_pass}",
        f"- selected_universe_count: {len(selected_symbols)} (min={int(args.min_symbols)})",
        f"- requested_ok: {requested_ok}",
        f"- requested_skipped: {requested_skipped}",
        f"- substituted: {len(substituted)}",
        f"- altdata_error_fail_closed: {int((altdata_payload.get('counts') or {}).get('error_fail_closed', 0))}",
        "",
        "## Top Skip Reasons",
    ]
    for k, v in sorted(universe["requested_reason_counts"].items(), key=lambda kv: (-int(kv[1]), str(kv[0]))):
        lines.append(f"- {k}: {v}")
    lines.extend(
        [
            "",
            "## Next Actions",
            f"- {summary['next_actions']}",
        ]
    )
    (out_dir / "readiness_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    if not readiness_pass:
        _write_json(out_dir / "fail_closed.json", {"pass": False, "summary": summary})

    subprocess.run(["git", "status", "--porcelain=v1"], check=False, stdout=(out_dir / "status_after.txt").open("w", encoding="utf-8"))
    subprocess.run(["git", "diff", "--stat"], check=False, stdout=(out_dir / "git_diff_stat_global_after.txt").open("w", encoding="utf-8"))
    _write_hashes(out_dir)
    return {"out": str(out_dir), "summary": summary}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Finish v0.0.0 paper-ready foundation using local resolver-valid data only.")
    p.add_argument("--requested-symbols", required=True)
    p.add_argument("--required-tfs", required=True)
    p.add_argument("--min-nonzero-bytes", type=int, default=1024)
    p.add_argument("--min-symbols", type=int, default=20)
    p.add_argument("--max-symbols", type=int, default=50)
    p.add_argument("--runtime-cap-seconds", type=int, default=900)
    p.add_argument("--profiles", default="low,mid,target")
    p.add_argument("--export-series-out", action="store_true", default=False)
    p.add_argument("--export-on-timeout", action="store_true", default=False)
    p.add_argument("--decision-trace-out", action="store_true", default=False)
    p.add_argument("--out", default=None)
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        result = run(args)
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as exc:
        out = Path(args.out) if args.out else Path("octa/var/evidence") / f"v000_finish_paper_ready_local_only_{_utc_now()}"
        out.mkdir(parents=True, exist_ok=True)
        _write_json(out / "fail_closed.json", {"pass": False, "reason": str(exc)})
        subprocess.run(["git", "status", "--porcelain=v1"], check=False, stdout=(out / "status_after.txt").open("w", encoding="utf-8"))
        subprocess.run(["git", "diff", "--stat"], check=False, stdout=(out / "git_diff_stat_global_after.txt").open("w", encoding="utf-8"))
        _write_hashes(out)
        print(json.dumps({"error": str(exc), "out": str(out)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
