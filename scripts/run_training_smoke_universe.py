#!/usr/bin/env python3
"""Training smoke runner: runs autopilot on a test config and produces an evidence pack.

Usage:
    python scripts/run_training_smoke_universe.py --config configs/autopilot_test_50.yaml
    python scripts/run_training_smoke_universe.py --config configs/autopilot_test_100.yaml \
        --evidence-dir octa/var/evidence/testlauf_b_100

Produces (under evidence dir):
    run_summary.md
    gate_pass_rates.json
    any_failures_top_reasons.json
    proof_no_network.json
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Training smoke universe runner")
    p.add_argument("--config", required=True, help="Autopilot config YAML (e.g. configs/autopilot_test_50.yaml)")
    p.add_argument("--evidence-dir", default=None,
                   help="Directory to write evidence pack (default: auto-named under octa/var/evidence/)")
    p.add_argument("--dry-run", action="store_true", help="Skip autopilot execution; parse existing run_id artifacts only")
    p.add_argument("--run-id", default=None, help="Existing run_id to parse (implies --dry-run)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Step 1: proof of no network
# ---------------------------------------------------------------------------

def _proof_no_network(cfg: Dict[str, Any], altdat_cfg_path: Path) -> Dict[str, Any]:
    """Collect evidence that no live network calls were made during training."""
    altdat_cfg: Dict[str, Any] = {}
    if altdat_cfg_path.exists():
        with open(altdat_cfg_path, encoding="utf-8") as f:
            altdat_cfg = yaml.safe_load(f) or {}

    offline_only_config = bool(altdat_cfg.get("offline_only", False))
    offline_only_env = os.environ.get("OKTA_ALTDATA_OFFLINE_ONLY", "")
    offline_only_effective = offline_only_config or offline_only_env.strip() == "1"

    global_gate = cfg.get("global_gate") or {}
    fred_enabled = bool(global_gate.get("fred_enabled", False))
    edgar_enabled = bool(global_gate.get("edgar_enabled", False))

    proof = {
        "verdict": "NO_NETWORK" if (offline_only_effective and not fred_enabled and not edgar_enabled) else "UNVERIFIED",
        "altdat_offline_only_config": offline_only_config,
        "altdat_offline_only_env": offline_only_env or "(unset)",
        "altdat_offline_only_effective": offline_only_effective,
        "global_gate_fred_enabled": fred_enabled,
        "global_gate_edgar_enabled": edgar_enabled,
        "altdat_config_path": str(altdat_cfg_path),
        "notes": [
            "Phase 1A fix: config/altdat.yaml offline_only: true blocks live FRED fetch in feature_builder.py",
            "Guard: fetch_fred_series() only called when offline_only=False (feature_builder.py:200)",
            "Guard: fred_enabled=false in global_gate prevents macro feature live fetch in main pipeline",
            "Guard: edgar_enabled=false in global_gate prevents EDGAR live fetch",
        ],
    }
    if proof["verdict"] != "NO_NETWORK":
        proof["warning"] = (
            "offline_only or fred/edgar gate not fully enforced — review altdat config and global_gate settings"
        )
    return proof


# ---------------------------------------------------------------------------
# Step 2: parse run artifacts
# ---------------------------------------------------------------------------

def _find_run_dir(run_id: str, registry_root: str = "artifacts") -> Optional[Path]:
    d = Path(registry_root) / "runs" / run_id
    return d if d.is_dir() else None


def _parse_killer_reason_counts(run_dir: Path) -> Dict[str, Any]:
    p = run_dir / "killer_reason_counts.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_structural_gate_summary(run_dir: Path) -> Dict[str, Any]:
    p = run_dir / "structural_gate_summary.md"
    if not p.exists():
        return {}
    text = p.read_text(encoding="utf-8")
    result: Dict[str, Any] = {}
    for line in text.splitlines():
        line = line.strip().lstrip("- ")
        if ":" in line:
            k, _, v = line.partition(":")
            try:
                result[k.strip()] = int(v.strip())
            except ValueError:
                result[k.strip()] = v.strip()
    return result


def _parse_stage_progress(run_dir: Path) -> List[Dict[str, Any]]:
    p = run_dir / "stage_progress.jsonl"
    if not p.exists():
        return []
    events = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except Exception:
            pass
    return events


def _parse_global_gate_status(run_dir: Path) -> Dict[str, Any]:
    p = run_dir / "global_gate_status.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_population_stats(run_dir: Path) -> Dict[str, Any]:
    p = run_dir / "population_stats.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_reason_counts_after(run_dir: Path) -> Dict[str, Any]:
    p = run_dir / "reason_counts_after.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _collect_per_symbol_outcomes(run_dir: Path) -> Dict[str, Any]:
    """Walk per_symbol/ to tally training outcomes per TF."""
    per_sym = run_dir / "per_symbol"
    if not per_sym.is_dir():
        return {}
    results: Dict[str, Dict[str, int]] = {}
    for sym_dir in sorted(per_sym.iterdir()):
        if not sym_dir.is_dir():
            continue
        sym = sym_dir.name
        for tf_dir in sorted(sym_dir.iterdir()):
            if not tf_dir.is_dir():
                continue
            tf = tf_dir.name
            if tf not in results:
                results[tf] = {"trained": 0, "timeout": 0, "gate_fail": 0, "other_fail": 0}
            progress_file = tf_dir / "train_step_progress.jsonl"
            if not progress_file.exists():
                continue
            last_event: Optional[Dict[str, Any]] = None
            for line in progress_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    last_event = json.loads(line)
                except Exception:
                    pass
            if last_event:
                details = last_event.get("details") or {}
                # status lives in details.status (e.g. "GATE_FAIL", "GATE_PASS", "TIMEOUT")
                status = str(details.get("status", last_event.get("status", ""))).upper()
                step = str(last_event.get("step", "")).lower()
                if "GATE_PASS" in status or step == "done":
                    results[tf]["trained"] += 1
                elif "TIMEOUT" in status or "timeout" in step:
                    results[tf]["timeout"] += 1
                elif "GATE_FAIL" in status or "FAIL" in status:
                    results[tf]["gate_fail"] += 1
                else:
                    results[tf]["other_fail"] += 1
    return results


# ---------------------------------------------------------------------------
# Step 3: build output documents
# ---------------------------------------------------------------------------

def _build_gate_pass_rates(
    global_gate: Dict[str, Any],
    structural: Dict[str, Any],
    killer_counts: Dict[str, Any],
    per_sym_outcomes: Dict[str, Any],
) -> Dict[str, Any]:
    universe_size = int(global_gate.get("universe_size", 0) or 0)
    global_pass = int(global_gate.get("n_pass", 0) or 0)
    global_fail = int(global_gate.get("n_fail", 0) or 0)
    global_pass_rate = round(global_pass / universe_size, 3) if universe_size > 0 else None

    struct_evaluated = int(structural.get("evaluated_rows", 0) or 0)
    struct_pass = int(structural.get("structural_pass", 0) or 0)
    struct_fail = int(structural.get("structural_fail", 0) or 0)
    struct_pass_rate = round(struct_pass / struct_evaluated, 3) if struct_evaluated > 0 else None

    training_by_tf: Dict[str, Any] = {}
    for tf, counts in per_sym_outcomes.items():
        total = sum(counts.values())
        trained = counts.get("trained", 0)
        training_by_tf[tf] = {
            "trained": trained,
            "timeout": counts.get("timeout", 0),
            "gate_fail": counts.get("gate_fail", 0),
            "other_fail": counts.get("other_fail", 0),
            "total_attempted": total,
            "success_rate": round(trained / total, 3) if total > 0 else None,
        }

    return {
        "global_gate": {
            "universe_size": universe_size,
            "pass": global_pass,
            "fail": global_fail,
            "pass_rate": global_pass_rate,
        },
        "structural_gate": {
            "evaluated": struct_evaluated,
            "pass": struct_pass,
            "fail": struct_fail,
            "pass_rate": struct_pass_rate,
        },
        "training_by_tf": training_by_tf,
        "killer_reason_counts": killer_counts,
    }


def _build_top_reasons(killer_counts: Dict[str, Any], reason_counts_after: Dict[str, Any]) -> Dict[str, Any]:
    # Flatten killer reasons across all TFs
    flat: Dict[str, int] = {}
    for tf_reasons in killer_counts.values():
        if isinstance(tf_reasons, dict):
            for reason, cnt in tf_reasons.items():
                flat[str(reason)] = flat.get(str(reason), 0) + int(cnt or 0)
    sorted_reasons = sorted(flat.items(), key=lambda x: -x[1])

    return {
        "killer_reasons_total": flat,
        "top_10_reasons": [{"reason": r, "count": c} for r, c in sorted_reasons[:10]],
        "reason_counts_after": reason_counts_after,
    }


def _build_run_summary_md(
    config_path: str,
    run_id: str,
    cfg: Dict[str, Any],
    gate_pass_rates: Dict[str, Any],
    top_reasons: Dict[str, Any],
    proof_net: Dict[str, Any],
    evidence_dir: Path,
) -> str:
    limit = (cfg.get("universe") or {}).get("limit", "?")
    runtime_profile = cfg.get("runtime_profile", "default")
    max_train = (cfg.get("training_budget") or {}).get("max_train_symbols_per_tf", {})
    gg = gate_pass_rates.get("global_gate") or {}
    sg = gate_pass_rates.get("structural_gate") or {}

    lines = [
        f"# Training Smoke Universe Run Summary",
        f"",
        f"- **config**: {config_path}",
        f"- **run_id**: {run_id}",
        f"- **universe limit**: {limit}",
        f"- **runtime_profile**: {runtime_profile}",
        f"- **max_train_symbols_per_tf**: {json.dumps(max_train)}",
        f"- **evidence_dir**: {evidence_dir}",
        f"",
        f"## Network Safety",
        f"",
        f"- verdict: **{proof_net['verdict']}**",
        f"- altdat offline_only (config): {proof_net['altdat_offline_only_config']}",
        f"- altdat offline_only (env): {proof_net['altdat_offline_only_env']}",
        f"- global_gate fred_enabled: {proof_net['global_gate_fred_enabled']}",
        f"- global_gate edgar_enabled: {proof_net['global_gate_edgar_enabled']}",
        f"",
        f"## Gate Pass Rates",
        f"",
        f"| Gate | Input | Pass | Fail | Pass Rate |",
        f"|------|-------|------|------|-----------|",
        f"| Global | {gg.get('universe_size', '?')} | {gg.get('pass', '?')} | {gg.get('fail', '?')} | {gg.get('pass_rate', '?')} |",
        f"| Structural | {sg.get('evaluated', '?')} | {sg.get('pass', '?')} | {sg.get('fail', '?')} | {sg.get('pass_rate', '?')} |",
        f"",
        f"## Training Outcomes by TF",
        f"",
        f"| TF | Trained | Timeout | Gate Fail | Other | Total | Success Rate |",
        f"|----|---------|---------|-----------|-------|-------|-------------|",
    ]
    for tf, counts in sorted(gate_pass_rates.get("training_by_tf", {}).items()):
        lines.append(
            f"| {tf} | {counts.get('trained','?')} | {counts.get('timeout','?')} | "
            f"{counts.get('gate_fail','?')} | {counts.get('other_fail','?')} | "
            f"{counts.get('total_attempted','?')} | {counts.get('success_rate','?')} |"
        )

    top10 = top_reasons.get("top_10_reasons") or []
    if top10:
        lines += [
            f"",
            f"## Top Failure Reasons (all TFs combined)",
            f"",
            f"| Rank | Reason | Count |",
            f"|------|--------|-------|",
        ]
        for i, item in enumerate(top10, 1):
            lines.append(f"| {i} | {item['reason']} | {item['count']} |")

    lines += [
        f"",
        f"---",
        f"Generated: {_now_utc()}",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"ERROR: config not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    cfg = _load_yaml(config_path)
    run_id = args.run_id

    # Step 1: run autopilot (unless dry-run or run_id provided)
    if run_id:
        print(f"[run_training_smoke_universe] Using existing run_id: {run_id}")
        dry_run = True
    else:
        dry_run = args.dry_run

    if not dry_run:
        ts = _now_utc()
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        cmd = [
            sys.executable,
            str(Path(__file__).parent / "octa_autopilot.py"),
            "--config", str(config_path),
        ]
        log_file = Path(f"/tmp/smoke_universe_{ts}.log")
        print(f"[run_training_smoke_universe] Launching autopilot: {' '.join(cmd)}")
        print(f"[run_training_smoke_universe] Log: {log_file}")

        with open(log_file, "w") as lf:
            proc = subprocess.run(cmd, env=env, stdout=lf, stderr=lf, check=False)

        print(f"[run_training_smoke_universe] Autopilot exit code: {proc.returncode}")
        if proc.returncode != 0:
            print(f"[run_training_smoke_universe] WARNING: non-zero exit code. Check {log_file}")

        # Discover run_id from the most recently created run dir
        registry_root = str(cfg.get("registry_root", "artifacts"))
        runs_dir = Path(registry_root) / "runs"
        if runs_dir.is_dir():
            run_dirs = sorted(
                [d for d in runs_dir.iterdir() if d.is_dir()],
                key=lambda d: d.stat().st_mtime,
                reverse=True,
            )
            if run_dirs:
                run_id = run_dirs[0].name
                print(f"[run_training_smoke_universe] Detected run_id: {run_id}")
    else:
        if not run_id:
            # discover most recent
            registry_root = str(cfg.get("registry_root", "artifacts"))
            runs_dir = Path(registry_root) / "runs"
            if runs_dir.is_dir():
                run_dirs = sorted(
                    [d for d in runs_dir.iterdir() if d.is_dir()],
                    key=lambda d: d.stat().st_mtime,
                    reverse=True,
                )
                if run_dirs:
                    run_id = run_dirs[0].name

    if not run_id:
        print("[run_training_smoke_universe] ERROR: could not determine run_id", file=sys.stderr)
        sys.exit(1)

    # Step 2: resolve evidence dir
    if args.evidence_dir:
        evidence_dir = Path(args.evidence_dir)
    else:
        label = config_path.stem  # e.g. "autopilot_test_50"
        evidence_dir = Path("octa") / "var" / "evidence" / f"smoke_{label}_{_now_utc()}"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    print(f"[run_training_smoke_universe] Evidence dir: {evidence_dir}")

    # Step 3: parse artifacts
    registry_root = str(cfg.get("registry_root", "artifacts"))
    run_dir = _find_run_dir(run_id, registry_root)
    if not run_dir:
        print(f"[run_training_smoke_universe] ERROR: run dir not found for {run_id}", file=sys.stderr)
        sys.exit(1)

    altdat_cfg_path = Path("config") / "altdat.yaml"
    proof_net = _proof_no_network(cfg, altdat_cfg_path)

    killer_counts = _parse_killer_reason_counts(run_dir)
    structural = _parse_structural_gate_summary(run_dir)
    global_gate_status = _parse_global_gate_status(run_dir)
    population_stats = _parse_population_stats(run_dir)
    reason_counts_after = _parse_reason_counts_after(run_dir)
    per_sym_outcomes = _collect_per_symbol_outcomes(run_dir)

    gate_pass_rates = _build_gate_pass_rates(
        global_gate_status, structural, killer_counts, per_sym_outcomes
    )
    top_reasons = _build_top_reasons(killer_counts, reason_counts_after)

    run_summary_md = _build_run_summary_md(
        str(config_path), run_id, cfg,
        gate_pass_rates, top_reasons, proof_net, evidence_dir
    )

    # Step 4: write evidence pack
    _write_json(evidence_dir / "gate_pass_rates.json", gate_pass_rates)
    _write_json(evidence_dir / "any_failures_top_reasons.json", top_reasons)
    _write_json(evidence_dir / "proof_no_network.json", proof_net)
    (evidence_dir / "run_summary.md").write_text(run_summary_md, encoding="utf-8")

    # Write a compact meta.json
    meta = {
        "run_id": run_id,
        "config": str(config_path),
        "run_dir": str(run_dir),
        "evidence_dir": str(evidence_dir),
        "generated_at": _now_utc(),
    }
    _write_json(evidence_dir / "meta.json", meta)

    print(f"[run_training_smoke_universe] Evidence written to {evidence_dir}")
    print(f"  gate_pass_rates.json")
    print(f"  any_failures_top_reasons.json")
    print(f"  proof_no_network.json")
    print(f"  run_summary.md")
    print(f"  meta.json")

    # Print summary to stdout
    print()
    print(run_summary_md)


if __name__ == "__main__":
    main()
