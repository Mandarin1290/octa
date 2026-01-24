from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dagster import op


@op(
    config_schema={
        "parquet_path": str,
        "target": str,
        "version": str,
    }
)
def run_trainer(context):
    repo_root = Path(__file__).resolve().parents[2]
    parquet_path = context.op_config["parquet_path"]
    target = context.op_config["target"]
    version = context.op_config["version"]
    cmd = [
        sys.executable,
        "-m",
        "scripts.train_and_save",
        "--parquet",
        str(parquet_path),
        "--target",
        str(target),
        "--version",
        str(version),
    ]
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    context.log.info(f"Invoking: {cmd} (cwd={repo_root})")
    subprocess.check_call(cmd, cwd=str(repo_root), env=env)
