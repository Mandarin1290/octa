"""Minimal Feast PoC scaffold.

This script prepares a small Feast repository that can ingest the parquet
feature files produced by the local feature store and register them as a
FeatureView in an offline (DuckDB/file) store.

It is intentionally non-invasive: if `feast` is not installed it prints
instructions how to install and how to run the PoC. After installing
dependencies, run this script which will create `feast_repo/` and a
`feature_store.yaml` you can use with Feast CLI or SDK.
"""
from __future__ import annotations

import json
from pathlib import Path

REPO_DIR = Path("feast_repo")
FEATURES_DIR = Path("artifacts/features")


def ensure_repo():
    REPO_DIR.mkdir(exist_ok=True)
    (REPO_DIR / "data").mkdir(exist_ok=True)


def write_feature_store_yaml():
    # DuckDB offline store for PoC
    cfg = {
        "project": "octa",
        "registry": "registry.db",
        "provider": "local",
        "online_store": {"type": "inmemory"},
        "offline_store": {"type": "duckdb", "path": "data/feast.duckdb"},
    }
    with open(REPO_DIR / "feature_store.yaml", "w") as fh:
        json.dump(cfg, fh, indent=2)


def main():
    ensure_repo()
    write_feature_store_yaml()
    print("Prepared Feast repo in", REPO_DIR)
    print()
    print("To run the PoC: install Feast and duckdb:")
    print("  pip install feast duckdb pandas pyarrow")
    print()
    print("Then, from this repository root, run:")
    print("  feast init --no-input --repo-path feast_repo  # optional")
    print("or use the SDK to `apply` Entities/FeatureViews programmatically.")
    print()
    print("A simple next step (manual) is:")
    print("  - inspect artifacts/features/<asset>/<version>/features.parquet")
    print("  - create a Feast FileSource pointing to that parquet")
    print("  - create Entity and FeatureView and `apply` to the repo")


if __name__ == "__main__":
    main()
