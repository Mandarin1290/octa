"""Collect OpenSky ADS-B snapshots and store aviation macro proxy features.

MANDATORY constraints (project policy):
- Data source: OpenSky Network (https://opensky-network.org/api/states/all)
- No authentication (free tier)
- No scraping, no reverse engineering
- No single-flight history persisted: only regional aggregates
- Interval: <= 5 minutes (backs off to 10-15 minutes on errors)
- Output: Parquet (snappy), UTC timestamps

This collector is optional and may be disabled at any time.
It must not affect core trading if it is down.

Run:
  python -m scripts.collect_aviation_opensky --base-dir /home/n-b/Octa/altdata/aviation

Dependencies (EXPLICIT, do not add more):
  pip install requests pandas numpy pyarrow fastparquet pytz tqdm
"""

from __future__ import annotations

import argparse
from pathlib import Path

from octa_altdata.aviation import _run_collector_loop


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--base-dir",
        default="/home/n-b/Octa/altdata/aviation",
        help="Output directory root for parquet files.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    _run_collector_loop(Path(args.base_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
