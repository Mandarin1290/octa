from __future__ import annotations

import importlib.util
from pathlib import Path

from dagster import repository


def _load_local_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"failed to create module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_HERE = Path(__file__).resolve().parent
_jobs_mod = _load_local_module("octa_dagster_jobs", _HERE / "jobs.py")
train_job = _jobs_mod.train_job
cascade_job = _jobs_mod.cascade_job


@repository
def octa_repo():
    return [train_job, cascade_job]
