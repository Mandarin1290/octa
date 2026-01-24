from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import yaml

from octa_core.bootstrap.deps import (
    ensure_installed,
    select_required_packages,
    verify_versions,
)
from octa_core.bootstrap.env_check import run_env_checks
from octa_core.bootstrap.start_guard import can_start_live, can_start_paper
from octa_core.security.audit import AuditLog
from octa_core.security.secrets import get_secret


def _load_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}


def _audit_path(cfg: dict) -> str:
    feats = cfg.get("features") or {}
    sec = feats.get("security") if isinstance(feats.get("security"), dict) else {}
    audit = sec.get("audit_log") if isinstance(sec.get("audit_log"), dict) else {}
    return str(audit.get("path", "artifacts/security/audit.jsonl"))


def _ts() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def _apply_gate_overlay_if_enabled(cfg: dict) -> None:
    feats = cfg.get("features") or {}
    gate = feats.get("gate_tuning") if isinstance(feats.get("gate_tuning"), dict) else {}
    if not bool(gate.get("enabled", False)):
        return
    overlay_path = str(gate.get("overlay_path") or "")
    if not overlay_path:
        raise RuntimeError("gate_tuning_enabled_but_no_overlay_path")
    if not Path(overlay_path).exists():
        raise FileNotFoundError(overlay_path)

    # The training config loader reads this env var (opt-in) and applies overlay.
    os.environ["OCTA_GATE_OVERLAY_PATH"] = overlay_path


def _opengamma_env(cfg: dict) -> None:
    feats = cfg.get("features") or {}
    og = feats.get("opengamma") if isinstance(feats.get("opengamma"), dict) else {}
    if not bool(og.get("enabled", False)):
        return
    # Allow bearer token via secrets.
    og_cfg_path = "octa_core/config/opengamma.yaml"
    if Path(og_cfg_path).exists():
        og_cfg = _load_yaml(og_cfg_path)
        auth = (og_cfg.get("opengamma") or {}).get("auth") if isinstance((og_cfg.get("opengamma") or {}).get("auth"), dict) else {}
        if str(auth.get("mode")) == "bearer_env":
            env_key = str(auth.get("bearer_env", "OPENGAMMA_BEARER_TOKEN"))
            tok = get_secret(env_key, cfg={"security": _load_yaml("octa_core/config/security.yaml").get("security", {})})
            if tok:
                os.environ[env_key] = tok


def main() -> int:
    ap = argparse.ArgumentParser(description="OCTA wrapper: env checks + deps + start guards + run selected mode")
    ap.add_argument("--features", default="octa_core/config/octa_features.yaml")
    ap.add_argument("--mode", default=None, help="autopilot|training_daemon")
    ap.add_argument("--start", default="paper", help="paper|live")
    ap.add_argument("--autopilot-config", default="configs/autonomous_paper.yaml")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--no-auto-install", action="store_true")
    args = ap.parse_args()

    cfg = _load_yaml(args.features)
    features = cfg.get("features") or {}
    modes = cfg.get("modes") or {}
    thresholds = cfg.get("thresholds") or {}

    audit_path = _audit_path(cfg)
    alog = AuditLog(path=audit_path)

    # Environment checks (fail-closed on errors)
    env = run_env_checks(
        min_free_gb=int(thresholds.get("min_free_disk_gb", 10) or 10),
        max_data_dir_gb_warn=int(thresholds.get("max_data_dir_gb_warn", 200) or 200),
    )
    if not env.ok:
        alog.append(event_type="bootstrap.env_check_failed", payload={"errors": env.errors, "warnings": env.warnings, "facts": env.facts})
        print("ENV CHECK FAILED:")
        for e in env.errors:
            print("-", e)
        return 2

    for w in env.warnings:
        # do not use logging; keep wrapper output minimal
        print("[warn]", w)

    # Dependency checks + auto-install
    auto_install = bool(features.get("auto_install_deps", True)) and (not args.no_auto_install)
    pkgs = select_required_packages({"features": features, **cfg} if False else cfg)  # cfg already contains 'features'

    wv, ev = verify_versions(pkgs)
    if ev:
        alog.append(event_type="bootstrap.deps_missing", payload={"errors": ev, "warnings": wv})
        if auto_install:
            try:
                ensure_installed(pkgs)
            except Exception as e:
                alog.append(event_type="bootstrap.deps_install_failed", payload={"error": str(e)})
                raise
        else:
            print("Missing critical deps:")
            for e in ev:
                print("-", e)
            return 3

    if wv:
        alog.append(event_type="bootstrap.deps_warnings", payload={"warnings": wv})

    # Optional overlays and secret env wiring
    _apply_gate_overlay_if_enabled(cfg)
    _opengamma_env(cfg)

    # Mode selection
    mode = args.mode or str(modes.get("default_mode", "autopilot"))
    start_mode = str(args.start).lower()

    if start_mode not in {"paper", "live"}:
        raise SystemExit("--start must be paper|live")

    if start_mode == "paper":
        d = can_start_paper(features=features, thresholds=thresholds)
        if not d.ok:
            alog.append(event_type="bootstrap.start_denied", payload={"mode": "paper", "errors": d.errors, "warnings": d.warnings})
            print("START DENIED (paper):")
            for e in d.errors:
                print("-", e)
            return 4

    if start_mode == "live":
        # Live: health checks are performed by the execution/risk adapters, not here.
        d = can_start_live(features=features, modes=modes, thresholds=thresholds, ibkr_health_ok=None, opengamma_health_ok=None, accounting_enabled=bool((features.get("accounting") or {}).get("enabled", False)))
        if not d.ok:
            alog.append(event_type="bootstrap.start_denied", payload={"mode": "live", "errors": d.errors, "warnings": d.warnings})
            print("START DENIED (live):")
            for e in d.errors:
                print("-", e)
            return 5

    alog.append(event_type="bootstrap.start", payload={"mode": start_mode, "runner": mode, "features": {"gate_tuning": bool((features.get("gate_tuning") or {}).get("enabled", False))}})

    env2 = os.environ.copy()
    env2["PYTHONPATH"] = "."

    if mode == "autopilot":
        cmd = [sys.executable, "scripts/octa_autopilot.py", "--config", args.autopilot_config]
        if args.run_id:
            cmd.extend(["--run-id", args.run_id])
        cmd.append("--run-paper")
        return subprocess.call(cmd, env=env2)

    if mode == "training_daemon":
        # Non-invasive: reuse existing daemon for sample pipeline.
        cmd = [sys.executable, "scripts/auto_pipeline_daemon.py", "--once"]
        return subprocess.call(cmd, env=env2)

    raise SystemExit(f"unknown_mode:{mode}")


if __name__ == "__main__":
    raise SystemExit(main())
