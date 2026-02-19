#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import signal
from pathlib import Path

from octa.support.branding import (
    BRAND_NAME,
    PLATFORM_NAME,
    TAGLINE,
    print_banner_once,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Stop OCTA OS Brain")
    ap.add_argument("--var-root", default="octa/var")
    ap.add_argument("--version", action="store_true", default=False)
    ap.add_argument("--about", action="store_true", default=False)
    ap.add_argument("--no-banner", action="store_true", default=False)
    args = ap.parse_args()
    if args.version:
        print(f"{PLATFORM_NAME} OS")
        return 0
    if args.about:
        print(f"{BRAND_NAME} | {TAGLINE}")
        return 0

    print_banner_once(enabled=not args.no_banner)

    pid_path = Path(args.var_root) / "state" / "os_brain.pid"
    if not pid_path.exists():
        print("no_pid_file")
        return 2

    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except Exception:
        print("invalid_pid_file")
        return 3

    try:
        os.kill(pid, signal.SIGINT)
    except ProcessLookupError:
        print("process_not_found")
        if pid_path.exists():
            pid_path.unlink()
        return 4

    print(f"sent_sigint:{pid}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
