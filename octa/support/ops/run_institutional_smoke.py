from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import os

os.environ.setdefault("NUMBA_DISABLE_JIT", "1")
os.environ.setdefault("NUMBA_CACHE_DIR", "/tmp/numba_cache")
os.environ.setdefault("OCTA_INSTITUTIONAL_FAST", "1")

from octa.support.ops.run_institutional_train import run_institutional_train


def main() -> None:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--universe-size", type=int, default=5)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    summary = run_institutional_train(
        config_path="octa_training/config/training.yaml",
        universe_size=args.universe_size,
        timeframes=["1D", "1H", "30M", "5M", "1M"],
        seed=args.seed,
        bucket="smoke",
        parquet_root="raw",
        mode="paper",
    )
    run_id = summary.get("run_id", "unknown")
    summary_path = Path("octa") / "var" / "artifacts" / "summary" / run_id / "institutional_summary.json"
    assert summary_path.exists(), "summary missing"
    print("run_id:", run_id)
    print("summary_path:", summary_path)


if __name__ == "__main__":
    main()
