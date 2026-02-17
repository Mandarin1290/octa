from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from octa.execution.ibkr_runtime import IBKRRuntimeConfig, ensure_ibkr_running, ibkr_health
from octa_ops.autopilot.cascade_train import CascadePolicy, run_cascade_training


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_yaml(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"missing_config:{p}")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = dict(payload)
    row.setdefault("ts", _utc())
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def _probe_port(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=1.0):
            return True
    except OSError:
        return False


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _structural_perf_by_tf(decisions: list[dict[str, Any]], tf: str) -> tuple[bool, bool]:
    row = next((d for d in decisions if str(d.get("timeframe")) == tf and str(d.get("stage")) == "train"), None)
    if row is None:
        return False, False
    details = row.get("details") if isinstance(row.get("details"), dict) else {}
    structural = bool(details.get("structural_pass", False))
    performance = bool(details.get("performance_pass", False))
    return structural, performance


def run(config_path: str, out_dir: Path) -> dict[str, Any]:
    cfg = _load_yaml(config_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    discovery_payload = {
        "generated_utc": _utc(),
        "integration_points": [
            "octa.execution.ibkr_runtime",
            "octa.execution.ibkr_x11_autologin",
            "octa_ops.autopilot.cascade_train",
        ],
    }
    _write_json(out_dir / "discovery_ibkr.json", discovery_payload)

    ibkr_cfg = cfg.get("ibkr", {}) if isinstance(cfg.get("ibkr"), dict) else {}
    display = os.environ.get("DISPLAY", "")
    xdg = os.environ.get("XDG_SESSION_TYPE", "")
    use_xvfb = bool(ibkr_cfg.get("use_xvfb", False))
    x11_ok = bool(display) and (bool(use_xvfb) or xdg == "x11")

    disclaimer = None
    if not display:
        disclaimer = {
            "disclaimer_emitted": True,
            "disclaimer_code": "IBKR_X11_UNAVAILABLE",
            "action": "LOCK_EXECUTION_SHADOW_ONLY",
            "required_operator_action": "Set DISPLAY to an active X11/Xvfb session before enabling IBKR autologin/runtime.",
        }
    elif (not use_xvfb) and xdg != "x11":
        disclaimer = {
            "disclaimer_emitted": True,
            "disclaimer_code": "IBKR_X11_REQUIRED",
            "action": "LOCK_EXECUTION_SHADOW_ONLY",
            "required_operator_action": "Use an X11 session or set ibkr.use_xvfb=true with valid DISPLAY.",
        }

    host = str(ibkr_cfg.get("host", "127.0.0.1"))
    port = int(ibkr_cfg.get("port", 7497))
    port_reachable = _probe_port(host, port)
    if bool(ibkr_cfg.get("require_port", False)) and not port_reachable and disclaimer is None:
        disclaimer = {
            "disclaimer_emitted": True,
            "disclaimer_code": "IBKR_API_PORT_UNREACHABLE",
            "action": "LOCK_EXECUTION_SHADOW_ONLY",
            "required_operator_action": "Start TWS/Gateway and ensure API port is reachable.",
        }

    runtime_cfg = IBKRRuntimeConfig(
        mode=str(ibkr_cfg.get("mode", "tws")),
        tws_cmd=list(ibkr_cfg.get("tws_cmd", [])) if isinstance(ibkr_cfg.get("tws_cmd"), list) else None,
        gateway_cmd=list(ibkr_cfg.get("gateway_cmd", [])) if isinstance(ibkr_cfg.get("gateway_cmd"), list) else None,
        process_match_substring=str(ibkr_cfg.get("process_match", "") or "") or None,
        host=host,
        port=port,
    )

    runtime_start = {"ok": False, "skipped": True}
    if bool(ibkr_cfg.get("start_runtime", False)):
        runtime_start = ensure_ibkr_running(runtime_cfg)

    health = ibkr_health(runtime_cfg)
    health.update(
        {
            "display": display,
            "xdg_session_type": xdg,
            "x11_ok": x11_ok,
            "port_reachable": port_reachable,
            "runtime_start": runtime_start,
        }
    )
    _write_json(out_dir / "ibkr_health.json", health)

    autologin_cfg = ibkr_cfg.get("autologin", {}) if isinstance(ibkr_cfg.get("autologin"), dict) else {}
    autologin_enabled = bool(autologin_cfg.get("enabled", False))
    autologin_result: dict[str, Any] = {"enabled": autologin_enabled, "started": False}
    autologin_events = out_dir / "ibkr_autologin_events.jsonl"

    if autologin_enabled:
        cmd = [
            sys.executable,
            "-m",
            "octa.execution.ibkr_x11_autologin",
            "--run",
            "--db",
            str(autologin_cfg.get("db", "octa/var/runtime/ibkr_autologin.sqlite3")),
            "--timeout-sec",
            str(int(autologin_cfg.get("timeout_sec", 10))),
            "--events-path",
            str(autologin_events),
        ]
        if bool(autologin_cfg.get("keepalive", False)):
            cmd.append("--keepalive")
        if bool(autologin_cfg.get("dry_run", False)):
            cmd.append("--dry-run")
        cp = subprocess.run(cmd, capture_output=True, text=True, check=False)
        autologin_result = {
            "enabled": True,
            "started": True,
            "rc": int(cp.returncode),
            "stdout": cp.stdout or "",
            "stderr": cp.stderr or "",
        }
        if cp.returncode != 0 and disclaimer is None:
            disclaimer = {
                "disclaimer_emitted": True,
                "disclaimer_code": "IBKR_X11_UNAVAILABLE",
                "action": "LOCK_EXECUTION_SHADOW_ONLY",
                "required_operator_action": "Validate autologin profiles and X11 popup watcher dependencies.",
            }

    decisions_out: list[dict[str, Any]] = []
    metrics_out: dict[str, Any] = {}
    jobs = cfg.get("cascade_jobs", []) if isinstance(cfg.get("cascade_jobs"), list) else []
    cascade_order = [str(x) for x in (cfg.get("cascade_order") or ["1D", "1H", "30M", "5M", "1M"])]

    for job in jobs:
        if not isinstance(job, dict):
            continue
        symbol = str(job.get("symbol", "")).strip()
        asset_class = str(job.get("asset_class", "unknown")).strip() or "unknown"
        parquet_paths = job.get("parquet_paths", {}) if isinstance(job.get("parquet_paths"), dict) else {}
        if not symbol:
            continue
        decisions, metrics = run_cascade_training(
            run_id=str(cfg.get("run_id", _utc_compact())),
            config_path=str(cfg.get("training_config", "configs/dev.yaml")),
            symbol=symbol,
            asset_class=asset_class,
            parquet_paths={str(k): str(v) for k, v in parquet_paths.items()},
            cascade=CascadePolicy(order=cascade_order),
            safe_mode=True,
            reports_dir=str(out_dir),
            model_root=str(job.get("model_root")) if job.get("model_root") else None,
            config_overrides=job.get("config_overrides") if isinstance(job.get("config_overrides"), dict) else None,
        )
        for d in decisions:
            decisions_out.append(
                {
                    "symbol": d.symbol,
                    "timeframe": d.timeframe,
                    "stage": d.stage,
                    "status": d.status,
                    "reason": d.reason,
                    "details": d.details,
                }
            )
        metrics_out[symbol] = metrics

    decisions_sorted = sorted(decisions_out, key=lambda x: (str(x.get("symbol")), str(x.get("timeframe")), str(x.get("stage"))))
    _write_json(out_dir / "cascade_status_table.json", {"rows": decisions_sorted})

    symbol_rows: list[dict[str, Any]] = []
    symbols = sorted({str(r.get("symbol")) for r in decisions_sorted})
    for sym in symbols:
        sym_rows = [r for r in decisions_sorted if str(r.get("symbol")) == sym and str(r.get("stage")) == "train"]
        s1d, p1d = _structural_perf_by_tf(sym_rows, "1D")
        s1h, p1h = _structural_perf_by_tf(sym_rows, "1H")
        eligible_strict = bool(p1d and p1h)
        symbol_rows.append(
            {
                "symbol": sym,
                "structural_1D": s1d,
                "performance_1D": p1d,
                "structural_1H": s1h,
                "performance_1H": p1h,
                "eligible_strict": eligible_strict,
            }
        )

    _write_json(out_dir / "execution_eligible_universe.json", {"symbols": [r for r in symbol_rows if bool(r["eligible_strict"])]})

    structural_pass_count = sum(
        1
        for r in decisions_sorted
        if str(r.get("stage")) == "train" and isinstance(r.get("details"), dict) and bool(r["details"].get("structural_pass", False))
    )
    performance_pass_count = sum(
        1
        for r in decisions_sorted
        if str(r.get("stage")) == "train" and isinstance(r.get("details"), dict) and bool(r["details"].get("performance_pass", False))
    )

    summary = {
        "run_id": str(cfg.get("run_id", _utc_compact())),
        "generated_utc": _utc(),
        "ibkr_mode": "x11_xvfb",
        "ibkr_autologin_enabled": autologin_enabled,
        "ibkr_autologin_result": autologin_result,
        "disclaimer_emitted": bool(disclaimer is not None),
        "disclaimer_code": disclaimer.get("disclaimer_code") if isinstance(disclaimer, dict) else None,
        "train_rows": len([r for r in decisions_sorted if str(r.get("stage")) == "train"]),
        "structural_pass_count": int(structural_pass_count),
        "performance_pass_count": int(performance_pass_count),
        "eligible_count": len([r for r in symbol_rows if bool(r["eligible_strict"])]),
        "symbols": symbol_rows,
    }
    _write_json(out_dir / "summary.json", summary)
    _write_json(out_dir / "metrics_snapshot.json", metrics_out)

    if disclaimer is not None:
        _write_json(out_dir / "disclaimer.json", disclaimer)

    _append_jsonl(out_dir / "ibkr_autologin_events.jsonl", {"event_type": "watcher_summary", "result": autologin_result})

    files_for_hash = [
        out_dir / "summary.json",
        out_dir / "cascade_status_table.json",
        out_dir / "ibkr_health.json",
        out_dir / "discovery_ibkr.json",
        out_dir / "ibkr_autologin_events.jsonl",
    ]
    if (out_dir / "disclaimer.json").exists():
        files_for_hash.append(out_dir / "disclaimer.json")

    hashes = {p.name: _sha256(p) for p in sorted(files_for_hash, key=lambda x: x.name) if p.exists()}
    _write_json(out_dir / "hashes.json", hashes)
    with (out_dir / "hashes.txt").open("w", encoding="utf-8") as f:
        for name in sorted(hashes.keys()):
            f.write(f"{hashes[name]}  {name}\n")

    return {"out_dir": str(out_dir), "summary": summary}


def main() -> int:
    ap = argparse.ArgumentParser(description="v000 full-universe cascade structural progression runner")
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    out = Path(args.out) if args.out else Path("octa/var/evidence") / f"v000_full_universe_cascade_train_{_utc_compact()}"
    out.mkdir(parents=True, exist_ok=True)

    result = run(config_path=str(args.config), out_dir=out)
    print(json.dumps(result["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
