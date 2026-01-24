"""Materialize Feast feature views to the online Redis store and verify sample online reads.

This script will try to start docker compose (if available) to bring up Redis,
wait for Redis to be reachable, run a short materialize window, and then sample
online features to verify writes.
"""
from __future__ import annotations

import socket
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Feast import is done lazily inside `main()` so we can optionally monkeypatch
# the `redis` module with `fakeredis` when running a fallback verification.
FeatureStore = None


def is_redis_up(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def try_start_redis_compose() -> bool:
    # Try common docker compose commands
    cmds = [["docker", "compose", "-f", "docker-compose.redis.yml", "up", "-d"],
            ["docker-compose", "-f", "docker-compose.redis.yml", "up", "-d"]]
    for cmd in cmds:
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
        except Exception:
            continue
    return False


def main():
    import argparse
    import sys

    p = argparse.ArgumentParser()
    p.add_argument("--use-fakeredis", action="store_true", help="Use fakeredis in-process instead of a real Redis server")
    args = p.parse_args()

    # optional fakeredis fallback: insert a compatible `redis` module before Feast imports
    if args.use_fakeredis:
        try:
            import fakeredis
            import redis as real_redis
        except Exception as e:
            raise SystemExit("fakeredis (and redis) are required for --use-fakeredis; please install them in the environment") from e
        # Keep the real `redis` module but inject a small `redis.asyncio` module that maps
        # the async client classes to fakeredis' FakeStrictRedis to satisfy Feast imports.
        import types
        async_mod = types.ModuleType("redis.asyncio")
        async_mod.StrictRedis = fakeredis.FakeStrictRedis
        async_mod.Redis = fakeredis.FakeStrictRedis
        sys.modules["redis.asyncio"] = async_mod
        # also make sync constructors point to fakeredis for safety
        real_redis.StrictRedis = fakeredis.FakeStrictRedis
        real_redis.Redis = fakeredis.FakeStrictRedis

    from feast import FeatureStore
    repo = FeatureStore(repo_path="feast_repo")

    # No runtime monkeypatching here — permanent sanitization is handled when
    # originating Parquet files are prepared (see `scripts/feast_apply.py`).

    # ensure Redis available (try start via docker-compose) unless using fakeredis fallback
    host = "localhost"
    port = 6379
    if args.use_fakeredis:
        print("Using fakeredis in-process — skipping Docker checks and binding to localhost:6379.")
    else:
        if not is_redis_up(host, port):
            print("Redis not reachable on localhost:6379 — checking Docker availability...")
            import shutil
            if shutil.which("docker") is None:
                print("Docker not found on PATH. Please start Redis manually or install Docker.")
                print("Example: docker compose -f docker-compose.redis.yml up -d")
                print("Or configure a different online_store in feast_repo/feature_store.yaml.")
                return

            print("Trying to start Redis via docker-compose...")
            ok = try_start_redis_compose()
            if not ok:
                print("Could not start Redis via docker-compose. Please start Redis manually.")
                return

            # wait for redis to be ready
            for _ in range(60):
                if is_redis_up(host, port):
                    break
                time.sleep(1)
            else:
                print("Redis did not start in time.")
                return

    print("Redis is up — proceeding to materialize to online store.")

    # small materialization window: last 7 days
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)

    print(f"Materializing from {start.isoformat()} to {end.isoformat()}")
    repo.materialize(start, end)

    # sample an online read for first FV
    fvs = repo.list_feature_views()
    if not fvs:
        print("No FeatureViews found in feast_repo")
        return

    fv = fvs[0]
    join_keys = [ec.name for ec in fv.entity_columns]
    files = list(Path("feast_repo").glob("data/*.parquet"))
    if not files:
        print("No parquet files under feast_repo/data to sample entities from")
        return

    import pandas as pd
    sample_df = pd.read_parquet(files[0])
    ent_col = join_keys[0] if join_keys else sample_df.columns[0]
    sample_vals = sample_df[ent_col].dropna().unique()[:5].tolist()
    entity_rows = [{ent_col: v} for v in sample_vals]

    refs = [fv.name + ":" + f.name for f in fv.schema if f.name not in set(join_keys + ["event_timestamp"])][:10]
    print("Querying online features for refs:", refs)
    try:
        online_resp = repo.get_online_features(features=refs, entity_rows=entity_rows)
        df = online_resp.to_df()
        out_path = Path("artifacts/feast_online_sample.json")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(df.to_json(orient="records", date_format="iso"))
        print("Wrote online sample to", out_path)
        print(df.head().to_string())
    except Exception as e:
        print("Online read failed:", e)


if __name__ == "__main__":
    main()
