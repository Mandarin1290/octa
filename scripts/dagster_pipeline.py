"""Dagster pipeline scaffold to orchestrate snapshot→feature→train→validate→promote.

This job runs existing scripts using subprocess so it works without deep
integration. Install `dagster` to run locally:

  pip install dagster dagit

Then start UI with `dagit -f scripts/dagster_pipeline.py` and run the job.
"""
from __future__ import annotations

import subprocess

from dagster import OpExecutionContext, job, op


def _run_cmd(cmd: list[str]) -> None:
    print("running:", " ".join(cmd))
    subprocess.check_call(cmd)


@op
def snapshot_op(context: OpExecutionContext):
    # call batch snapshot script for a single asset example
    _run_cmd(["python3", "scripts/record_dataset_version.py", "--asset", "6A", "--max-rows", "1000"])


@op
def features_op(context: OpExecutionContext):
    _run_cmd(["python3", "scripts/feature_store.py", "--materialize", "6A", "--max-rows", "1000"])


@op
def train_op(context: OpExecutionContext):
    _run_cmd(["python3", "scripts/train_on_raw.py", "--asset", "6A", "--max-rows", "1000"])


@op
def validate_and_promote_op(context: OpExecutionContext):
    _run_cmd(["python3", "scripts/batch_train_assets.py", "--max-assets", "1", "--max-rows", "1000"])


@job
def octa_pipeline():
    snapshot_op()
    features_op()
    train_op()
    validate_and_promote_op()
