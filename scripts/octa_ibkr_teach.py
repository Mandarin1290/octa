#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys


def main() -> int:
    ap = argparse.ArgumentParser(description="Convenience wrapper for IBKR X11 autologin teach/list/run.")
    ap.add_argument("--db", default="octa/var/runtime/ibkr_autologin.sqlite3")

    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--teach", action="store_true", default=False)
    mode.add_argument("--list", action="store_true", default=False)
    mode.add_argument("--run-dry", action="store_true", default=False)

    ap.add_argument("--profile-name", default="tws_disclaimer")
    ap.add_argument("--timeout-sec", type=int, default=60)
    args = ap.parse_args()

    base = [sys.executable, "-m", "octa.execution.ibkr_x11_autologin", "--db", str(args.db)]
    if args.teach:
        cmd = base + ["--teach", "--profile-name", str(args.profile_name)]
    elif args.list:
        cmd = base + ["--list-profiles"]
    else:
        cmd = base + ["--run", "--dry-run", "--timeout-sec", str(int(args.timeout_sec))]

    cp = subprocess.run(cmd, check=False)
    return int(cp.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
