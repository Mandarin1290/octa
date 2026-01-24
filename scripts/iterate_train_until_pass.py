#!/usr/bin/env python3
"""Iterative real-data training until PASS (no simulation).

Runs train_multiframe_symbol with a layer subset and progressively increases Optuna trials.
Stops when all selected layers pass or max runs reached.

Example:
  python3 scripts/iterate_train_until_pass.py \
    --symbol DJR \
    --base-config configs/tmp_train_djr_multitf_hf.yaml \
    --layers daily,struct_1h \
    --base-run-id djr_iter \
    --trials 200,400,800 \
    --mode live
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except Exception:
    yaml = None

REPO_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = REPO_ROOT / "reports"
AUTO_CFG_DIR = REPO_ROOT / "configs" / "_auto"


def _now_tag() -> str:
    return datetime.now(timezone.utc).isoformat().replace(":", "-")


def _load_yaml(path: Path) -> dict:
    if yaml is None:
        raise RuntimeError("pyyaml not installed; cannot load config")
    return yaml.safe_load(path.read_text()) or {}


def _save_yaml(path: Path, obj: dict) -> None:
    if yaml is None:
        raise RuntimeError("pyyaml not installed; cannot write config")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(obj, sort_keys=False))


def _set_nested(d: dict, keys: list[str], value):
    cur = d
    for k in keys[:-1]:
        if k not in cur or not isinstance(cur[k], dict):
            cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = value


def _run_once(symbol: str, cfg_path: Path, run_id: str, mode: str, layers: str | None, continue_on_fail: bool) -> tuple[int, str]:
    cmd = [
        "python3",
        str(REPO_ROOT / "scripts" / "train_multiframe_symbol.py"),
        "--symbol",
        symbol,
        "--config",
        str(cfg_path),
        "--run-id",
        run_id,
        "--mode",
        mode,
    ]
    if continue_on_fail:
        cmd.append("--continue-on-fail")
    if layers:
        cmd.extend(["--layers", layers])

    # Stream output live (monitoring) while also buffering for report path extraction.
    buf: list[str] = []
    p = subprocess.Popen(
        cmd,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert p.stdout is not None
    for line in p.stdout:
        print(line, end="")
        buf.append(line)
    rc = p.wait()
    return rc, "".join(buf)


def _extract_report_path(output: str) -> Path | None:
    marker = "Wrote multi-timeframe report:"
    for line in output.splitlines():
        if marker in line:
            p = line.split(marker, 1)[1].strip()
            try:
                return Path(p)
            except Exception:
                return None
    return None


def _all_layers_passed(report_path: Path, layers_csv: str) -> tuple[bool, list[str]]:
    layers = [s.strip() for s in (layers_csv or "").split(",") if s.strip()]
    data = json.loads(report_path.read_text())
    reasons = []
    ok = True
    for layer in layers:
        layer_obj = (data.get("layers") or {}).get(layer)
        if not isinstance(layer_obj, dict):
            ok = False
            reasons.append(f"missing_layer:{layer}")
            continue
        passed = bool(layer_obj.get("passed"))
        if not passed:
            ok = False
            rs = layer_obj.get("gate_result", {}).get("reasons")
            if isinstance(rs, list) and rs:
                reasons.append(f"{layer}:" + ";".join([str(x) for x in rs[:3]]))
            else:
                reasons.append(f"{layer}:failed")
    return ok, reasons


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--base-config", required=True)
    ap.add_argument("--layers", required=True, help='e.g. "daily,struct_1h"')
    ap.add_argument("--base-run-id", default="iter")
    ap.add_argument("--trials", default="200,400,800", help="Comma list")
    ap.add_argument("--mode", default="live", choices=["live", "train"])
    ap.add_argument("--continue-on-fail", action="store_true")
    args = ap.parse_args()

    base_cfg_path = Path(args.base_config)
    cfg = _load_yaml(base_cfg_path)

    trials_list = [int(x.strip()) for x in str(args.trials).split(",") if x.strip()]
    tag = _now_tag()

    AUTO_CFG_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    for i, trials in enumerate(trials_list, start=1):
        run_id = f"{args.base_run_id}_i{i}_t{trials}_{tag}"

        cfg_i = dict(cfg)
        _set_nested(cfg_i, ["tuning", "enabled"], True)
        _set_nested(cfg_i, ["tuning", "optuna_trials"], trials)
        # Scale timeout proportionally (cap at 2h to avoid runaway)
        timeout = min(7200, max(600, int(9 * trials)))
        _set_nested(cfg_i, ["tuning", "timeout_sec"], timeout)

        # Keep per-layer override in sync (struct_1h)
        if isinstance(cfg_i.get("layers"), dict) and isinstance(cfg_i["layers"].get("struct_1h"), dict):
            _set_nested(cfg_i, ["layers", "struct_1h", "tuning", "enabled"], True)
            _set_nested(cfg_i, ["layers", "struct_1h", "tuning", "optuna_trials"], trials)
            _set_nested(cfg_i, ["layers", "struct_1h", "tuning", "timeout_sec"], timeout)

        out_cfg = AUTO_CFG_DIR / f"{args.symbol}_{run_id}.yaml"
        _save_yaml(out_cfg, cfg_i)

        rc, out = _run_once(
            symbol=args.symbol,
            cfg_path=out_cfg,
            run_id=run_id,
            mode=args.mode,
            layers=args.layers,
            continue_on_fail=bool(args.continue_on_fail),
        )

        rep = _extract_report_path(out)
        if rep is None or not rep.exists():
            print(out)
            raise SystemExit(f"Run {run_id} did not produce a report (rc={rc}).")

        ok, reasons = _all_layers_passed(rep, args.layers)
        print(f"ITER {i}/{len(trials_list)} trials={trials} report={rep} passed={ok}")
        if reasons:
            print("Reasons:")
            for r in reasons:
                print("-", r)

        if ok:
            print("PASS achieved.")
            return

    raise SystemExit("No PASS within given trial schedule.")


if __name__ == "__main__":
    main()
