# Online Store (Redis) — Local verification

This document explains how to start a local Redis instance for the Feast online store and how to run the verification script.

Start Redis using Docker Compose (recommended):

```bash
docker compose -f docker-compose.redis.yml up -d
```

If Docker is not available, you can start Redis manually on the default port 6379.

Run the verification script which will materialize a small window and sample online reads:

```bash
PYTHONPATH=. python3 scripts/feast_materialize_online.py
```

If the script exits early with a message about Docker not being available, follow the Docker instructions above or configure a reachable `online_store` in `feast_repo/feature_store.yaml`.

Important: To avoid Feast/ibis schema validation errors ensure that any process producing Parquet files does NOT write the pandas index. Use:

```py
df.to_parquet(path, index=False)
```

The `scripts/feast_apply.py` now performs a final sanitization pass on the Parquet files it copies into `feast_repo/data/` to remove stray index metadata and reorder columns to match FeatureView schemas. Prefer updating upstream producers to write clean Parquet to avoid runtime workarounds.
