#!/usr/bin/env python3
"""Paper trading entrypoint — Tier-1 shadow trading via IBKR paper port.

Prerequisites:
  - TWS running on port 7496 (paper)
  - OCTA_ALLOW_PAPER_ORDERS=1
  - OCTA_BROKER_MODE=ib_insync

Without TWS: fails at pre-execution gate (fail-closed).
With OCTA_BROKER_MODE=sandbox (default): dry-run, no real orders.
"""
from __future__ import annotations

import datetime
import os
import sys
import time
from pathlib import Path


def main() -> int:
    run_id = f"shadow_{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
    log_dir = Path("octa/var/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    broker_mode = os.getenv("OCTA_BROKER_MODE", "sandbox")
    print(f"[paper_runner] run_id={run_id}  broker_mode={broker_mode}", flush=True)

    try:
        from octa_ops.autopilot.paper_runner import run_paper

        result = run_paper(
            run_id=run_id,
            config_path="configs/p03_research.yaml",
            registry_root="artifacts",
            ledger_dir="artifacts/ledger_paper",
            level="paper",
            live_enable=False,
            last_n_rows=300,
            paper_log_path=str(log_dir / "paper_trade_log.ndjson"),
            max_runtime_s=3600,
            broker_cfg_path="configs/execution_ibkr.yaml",
        )
    except Exception as exc:
        print(f"[paper_runner] FAILED: {exc}", file=sys.stderr, flush=True)
        return 1

    orders = result.get("orders_placed", [])
    skipped = result.get("skipped", [])
    errors = result.get("errors", [])
    print(
        f"[paper_runner] DONE  orders={len(orders)}  skipped={len(skipped)}  errors={len(errors)}",
        flush=True,
    )
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
