from __future__ import annotations

import argparse
from pathlib import Path

from octa.execution.runner import ExecutionConfig, run_execution


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--asset-class", default=None)
    p.add_argument("--max-symbols", type=int, default=0)
    p.add_argument("--run-id", default=None, help="Training run id override for selection")
    p.add_argument("--evidence-dir", default=None)
    p.add_argument("--loop", action="store_true", default=False)
    p.add_argument("--cycle-seconds", type=int, default=60)
    p.add_argument("--max-cycles", type=int, default=1)
    p.add_argument("--enable-carry", action="store_true", default=False)
    p.add_argument("--carry-config", default="octa/var/config/carry_config.json")
    p.add_argument("--carry-rates", default="octa/var/config/carry_rates.json")
    args = p.parse_args()

    cfg = ExecutionConfig(
        mode="dry-run",
        asset_class=args.asset_class,
        max_symbols=int(args.max_symbols),
        evidence_dir=Path(args.evidence_dir) if args.evidence_dir else Path("octa") / "var" / "evidence" / "execution_shadow",
        training_run_id=(str(args.run_id).strip() if args.run_id else None),
        loop=bool(args.loop),
        cycle_seconds=int(args.cycle_seconds),
        max_cycles=int(args.max_cycles),
        enable_carry=bool(args.enable_carry),
        carry_config_path=Path(args.carry_config),
        carry_rates_path=Path(args.carry_rates),
    )
    run_execution(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
