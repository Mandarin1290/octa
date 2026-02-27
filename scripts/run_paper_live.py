#!/usr/bin/env python3
"""Scheduled paper-execution entry point.

Invoked by octa-paper-runner.timer (every 30 min).
Pre-execution gate (TWS readiness + broker handshake) runs automatically
inside run_paper() when OCTA_BROKER_MODE=ib_insync.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from octa_ops.autopilot.paper_runner import run_paper

run_id = "paper_live_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

result = run_paper(
    run_id=run_id,
    config_path="configs/autonomous_paper.yaml",
    broker_cfg_path="configs/execution_ibkr.yaml",
    level="paper",
    live_enable=False,
)

print(json.dumps(result, indent=2, default=str))
