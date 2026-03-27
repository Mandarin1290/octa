#!/usr/bin/env python3
"""Shadow execution entry point for Phase 2 validation.

Runs the autopilot paper_runner with OCTA_BROKER_MODE=sandbox:
  - Uses promoted artifacts from artifacts/registry.sqlite3
  - Builds live features from refreshed parquets
  - Runs ML inference and logs decisions
  - Does NOT place real orders (sandbox broker)

Usage:
  python scripts/run_shadow_execution.py
  OCTA_BROKER_MODE=sandbox python scripts/run_shadow_execution.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from octa_ops.autopilot.paper_runner import run_paper
from octa_ops.autopilot.types import now_utc_iso


def main() -> int:
    # Ensure sandbox mode: this script must never place real orders
    if os.environ.get("OCTA_BROKER_MODE", "sandbox") not in ("sandbox", ""):
        print(f"ERROR: OCTA_BROKER_MODE={os.environ.get('OCTA_BROKER_MODE')} is not sandbox. "
              "This script is for shadow execution only.", file=sys.stderr)
        return 1
    os.environ["OCTA_BROKER_MODE"] = "sandbox"

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"shadow_{ts}"

    result = run_paper(
        run_id=run_id,
        config_path="configs/autonomous_paper.yaml",
        level="paper",
        live_enable=False,
        registry_root="artifacts",
        ledger_dir="artifacts/ledger_shadow",
        paper_log_path="artifacts/shadow_orders.ndjson",
        max_runtime_s=120,
        broker_cfg_path="configs/execution_ibkr.yaml",
    )

    summary = {
        "run_id": run_id,
        "ts": ts,
        "promoted_count": result["promoted_count"],
        "placed": len(result["placed"]),
        "skipped": len(result["skipped"]),
        "decisions": result["skipped"],
    }
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
