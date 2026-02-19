#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from octa.os import OSBrain, OSBrainConfig
from octa.os.state_store import OSStateStore
from octa.support.branding import (
    BRAND_NAME,
    PLATFORM_NAME,
    TAGLINE,
    print_banner_once,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Start OCTA OS Brain")
    ap.add_argument("--config", default="configs/dev.yaml")
    ap.add_argument("--policy", default="configs/policy.yaml")
    ap.add_argument("--mode", default="shadow", choices=["shadow", "paper", "live"])
    ap.add_argument("--arm-live", action="store_true", default=False)
    ap.add_argument("--once", action="store_true", default=False)
    ap.add_argument("--var-root", default="octa/var")
    ap.add_argument("--version", action="store_true", default=False)
    ap.add_argument("--about", action="store_true", default=False)
    ap.add_argument("--no-banner", action="store_true", default=False)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.version:
        print(f"{PLATFORM_NAME} OS")
        return 0
    if args.about:
        print(f"{BRAND_NAME} | {TAGLINE}")
        return 0

    print_banner_once(enabled=not args.no_banner)
    state = OSStateStore(Path(args.var_root))
    state.paths.pid_path.parent.mkdir(parents=True, exist_ok=True)
    state.paths.pid_path.write_text(str(os.getpid()) + "\n", encoding="utf-8")

    brain = OSBrain(
        OSBrainConfig(
            config_path=Path(args.config),
            policy_path=Path(args.policy),
            mode=str(args.mode),
            arm_live_flag=bool(args.arm_live),
        ),
        state_store=state,
    )

    try:
        while True:
            out = brain.tick()
            print(json.dumps(out, sort_keys=True))
            if args.once or brain.should_halt():
                break
            time.sleep(max(1, int(out.get("next_check_in_sec", 30))))
    finally:
        if state.paths.pid_path.exists():
            state.paths.pid_path.unlink()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
