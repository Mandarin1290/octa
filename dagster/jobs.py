from __future__ import annotations

import importlib.util
from pathlib import Path

from dagster import job


def _load_local_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"failed to create module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_HERE = Path(__file__).resolve().parent
_ops_train_mod = _load_local_module("octa_dagster_ops_train", _HERE / "ops" / "train.py")
_ops_cascade_mod = _load_local_module("octa_dagster_ops_cascade", _HERE / "ops" / "cascade.py")
run_trainer = _ops_train_mod.run_trainer
run_cascade_op = _ops_cascade_mod.run_cascade_op


@job
def train_job():
    repo_root = _HERE.parent
    sample = repo_root / "tests" / "data" / "sample_parquet.parquet"
    run_trainer.configured(
        {
            "parquet_path": str(sample),
            "target": "target",
            "version": "dagster-run-1",
        },
        name="run_trainer",
    )()


@job
def cascade_job():
    run_cascade_op()
