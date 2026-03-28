from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from octa.execution.runner import ExecutionConfig, run_execution


def _default_run_id() -> str:
    return f"execution_v0_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="dry-run", choices=["dry-run"])
    p.add_argument("--asset-class", default=None)
    p.add_argument("--max-symbols", type=int, default=0)
    p.add_argument("--run-id", default=None, help="Training run id override for selection")
    p.add_argument("--evidence-dir", default=None)
    p.add_argument("--loop", action="store_true", default=False)
    p.add_argument("--cycle-seconds", type=int, default=60)
    p.add_argument("--max-cycles", type=int, default=1)
    p.add_argument("--enable-live", action="store_true", default=False)
    p.add_argument("--i-understand-live-risk", action="store_true", default=False)
    p.add_argument("--enable-carry", action="store_true", default=False)
    p.add_argument("--carry-config", default="octa/var/config/carry_config.json")
    p.add_argument("--carry-rates", default="octa/var/config/carry_rates.json")
    p.add_argument("--enable-carry-live", action="store_true", default=False)
    p.add_argument("--i-understand-carry-risk", action="store_true", default=False)
    p.add_argument("--broker-cfg", default=None, help="IBKR broker config YAML for pre-execution gate")
    p.add_argument("--pre-execution", action="store_true", default=None, help="Force-enable pre-execution gate")
    p.add_argument("--no-pre-execution", action="store_false", dest="pre_execution", help="Force-disable pre-execution gate")
    args = p.parse_args()

    run_id = str(args.run_id).strip() if args.run_id else _default_run_id()
    evidence_dir = Path(args.evidence_dir) if args.evidence_dir else Path("octa") / "var" / "evidence" / run_id

    cfg = ExecutionConfig(
        mode=str(args.mode),
        asset_class=args.asset_class,
        max_symbols=int(args.max_symbols),
        evidence_dir=evidence_dir,
        training_run_id=(str(args.run_id).strip() if args.run_id else None),
        loop=bool(args.loop),
        cycle_seconds=int(args.cycle_seconds),
        max_cycles=int(args.max_cycles),
        enable_live=bool(args.enable_live),
        i_understand_live_risk=bool(args.i_understand_live_risk),
        enable_carry=bool(args.enable_carry),
        carry_config_path=Path(args.carry_config),
        carry_rates_path=Path(args.carry_rates),
        enable_carry_live=bool(args.enable_carry_live),
        i_understand_carry_risk=bool(args.i_understand_carry_risk),
        broker_cfg_path=Path(args.broker_cfg) if args.broker_cfg else None,
        pre_execution_enabled=args.pre_execution,
    )
    summary = run_execution(cfg)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
