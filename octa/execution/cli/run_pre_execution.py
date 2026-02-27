from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from octa.execution.notifier import ExecutionNotifier
from octa.execution.pre_execution import (
    PreExecutionError,
    load_pre_execution_settings,
    run_pre_execution_gate,
)


def _default_run_id() -> str:
    return f"pre_exec_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Run OCTA pre-execution gate only")
    ap.add_argument("--broker-cfg", default="configs/execution_ibkr.yaml")
    ap.add_argument("--mode", default="paper", choices=["shadow", "dry-run", "paper", "live"])
    ap.add_argument("--evidence-dir", default=None)
    ap.add_argument("--enabled", action="store_true", default=None, help="Force-enable pre-execution")
    ap.add_argument("--disabled", action="store_false", dest="enabled", help="Force-disable pre-execution")
    args = ap.parse_args()

    evidence_dir = Path(args.evidence_dir) if args.evidence_dir else Path("octa") / "var" / "evidence" / _default_run_id()
    evidence_dir.mkdir(parents=True, exist_ok=True)
    notifier = ExecutionNotifier(evidence_dir)

    settings = load_pre_execution_settings(Path(args.broker_cfg), mode=str(args.mode))
    if args.enabled is not None:
        settings = type(settings)(
            enabled=bool(args.enabled),
            tws_e2e_script=settings.tws_e2e_script,
            tws_e2e_env_passthrough=settings.tws_e2e_env_passthrough,
            tws_e2e_timeout_sec=settings.tws_e2e_timeout_sec,
            port_check=settings.port_check,
            handshake=settings.handshake,
            telegram=settings.telegram,
            ibkr_client_id=settings.ibkr_client_id,
        )
    try:
        out = run_pre_execution_gate(
            settings=settings,
            evidence_dir=evidence_dir,
            notifier=notifier,
            mode=args.mode,
            run_id=evidence_dir.name,
        )
        print(json.dumps(out, sort_keys=True))
        return 0
    except PreExecutionError as exc:
        print(json.dumps({"ready": False, "reason": exc.reason, "detail": exc.detail}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
