#!/usr/bin/env python3
"""Daemon to automatically convert sample CSVs and run the multi‑TF pipeline when new files arrive.

Usage:
  python scripts/auto_pipeline_daemon.py --once
  python scripts/auto_pipeline_daemon.py --poll-interval 60
"""
import argparse
import hashlib
import os
import subprocess
import time
from pathlib import Path

WATCH_DIR = Path("raw/frd_complete_plus_options_sample")
STATE_FILE = Path(".auto_pipeline_state")


def dir_hash(path: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(path.glob("*.csv")):
        try:
            st = p.stat()
            h.update(str(p.name).encode())
            h.update(str(st.st_mtime).encode())
            h.update(str(st.st_size).encode())
        except Exception:
            continue
    return h.hexdigest()


def last_seen() -> str:
    if STATE_FILE.exists():
        try:
            return STATE_FILE.read_text().strip()
        except Exception:
            return ""
    return ""


def write_seen(val: str) -> None:
    try:
        STATE_FILE.write_text(val)
    except Exception:
        pass


def run_pipeline():
    # run conversion and pipeline script
    print("[auto_pipeline] running conversion + pipeline")
    env = os.environ.copy()
    env.update({"PYTHONPATH": "."})
    subprocess.run(["python", "scripts/convert_and_run_sample_pipeline.py"], env=env)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--poll-interval", type=int, default=60, help="Poll interval in seconds")
    args = parser.parse_args()

    if not WATCH_DIR.exists():
        print(f"Watch directory {WATCH_DIR} not found")
        return

    current = dir_hash(WATCH_DIR)
    if args.once:
        # run unconditionally once
        run_pipeline()
        write_seen(current)
        return

    seen = last_seen()
    print(f"Starting auto pipeline watcher (poll {args.poll_interval}s). initial state: {seen[:8]}")
    while True:
        try:
            cur = dir_hash(WATCH_DIR)
            if cur != seen:
                print("Change detected in CSVs — triggering pipeline")
                run_pipeline()
                write_seen(cur)
                seen = cur
            time.sleep(args.poll_interval)
        except KeyboardInterrupt:
            print("Exiting auto pipeline watcher")
            break
        except Exception as e:
            print("Watcher error:", e)
            time.sleep(args.poll_interval)


if __name__ == '__main__':
    main()
