#!/usr/bin/env python3
"""Run scripts/train_multiframe_symbol.py over many symbols with bounded parallelism.

Why this exists:
- Spawning 20+ processes can thrash IO (each scans raw/). This runner caps concurrency.
- Writes one log per symbol and a pids.json manifest.

Usage:
  python scripts/run_batch_train_multiframe.py \
    --config configs/tmp_train_djr_multitf_hf_30m_hedgefund.yaml \
    --from-gate-report reports/gate_reports/<run_id>/gate_report.json \
    --max-parallel 4
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_symbols(args: argparse.Namespace) -> list[str]:
    if args.symbols_file:
        p = Path(args.symbols_file)
        syms = [line.strip().upper() for line in p.read_text().splitlines() if line.strip()]
        return sorted(dict.fromkeys(syms))

    if args.from_gate_report:
        g = json.loads(Path(args.from_gate_report).read_text())
        syms = sorted(g.get("symbols", {}).keys())
        return [str(s).strip().upper() for s in syms if str(s).strip()]

    raise SystemExit("Provide --symbols-file or --from-gate-report")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Training config yaml passed to train_multiframe_symbol.py")
    parser.add_argument("--run-id", default=None, help="Run id; default: stocks_30m_train_hf_<ts>")
    parser.add_argument("--max-parallel", type=int, default=4, help="Max concurrent processes")
    parser.add_argument("--python", default=str(Path(".venv/bin/python")), help="Python executable to use")
    parser.add_argument("--from-gate-report", default=None, help="Path to gate_report.json to derive symbols")
    parser.add_argument("--symbols-file", default=None, help="Text file with symbols (one per line)")
    args = parser.parse_args()

    run_id = args.run_id or f"stocks_30m_train_hf_{_utc_stamp()}"
    logs_dir = Path("reports/training_30m") / run_id
    logs_dir.mkdir(parents=True, exist_ok=True)

    symbols = _load_symbols(args)
    if not symbols:
        raise SystemExit("No symbols to run")

    python_exe = args.python
    if not Path(python_exe).exists():
        # allow absolute interpreter too
        if not ("/" in python_exe and Path(python_exe).exists()):
            raise SystemExit(f"Python executable not found: {python_exe}")

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    # Persist run manifest up-front.
    (logs_dir / ".run_id").write_text(run_id + "\n")
    (logs_dir / "runner.meta.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "config": args.config,
                "max_parallel": args.max_parallel,
                "symbols": symbols,
                "created_utc": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        )
        + "\n"
    )

    running: dict[str, subprocess.Popen] = {}
    procs: list[dict[str, object]] = []

    def start(sym: str) -> None:
        log_path = logs_dir / f"{sym}.log"
        fh = open(log_path, "wb", buffering=0)
        cmd = [
            python_exe,
            "-u",
            "scripts/train_multiframe_symbol.py",
            "--symbol",
            sym,
            "--run-id",
            run_id,
            "--config",
            args.config,
        ]
        p = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT, env=env, start_new_session=True)
        fh.close()
        running[sym] = p
        procs.append({"symbol": sym, "pid": p.pid, "log_path": str(log_path)})

    idx = 0
    while idx < len(symbols) or running:
        while idx < len(symbols) and len(running) < max(1, int(args.max_parallel)):
            sym = symbols[idx]
            idx += 1
            start(sym)
            time.sleep(0.2)

        # update pids.json periodically
        (logs_dir / "pids.json").write_text(
            json.dumps({"run_id": run_id, "config": args.config, "count": len(procs), "procs": procs}, indent=2)
            + "\n"
        )

        done = []
        for sym, p in running.items():
            rc = p.poll()
            if rc is not None:
                done.append(sym)
        for sym in done:
            running.pop(sym, None)

        time.sleep(1.0)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
