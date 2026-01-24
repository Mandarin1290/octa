from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

from dagster import RunRequest, ScheduleDefinition, repository, sensor

from .dagster_feast_pipeline import feast_etl_job

STATE_FILE = Path("artifacts/.dagster_sensor_state.json")


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save_state(d: dict):
    # atomic write to avoid concurrent corruption
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(d))
    tmp.replace(STATE_FILE)


def _latest_snapshot_hash() -> str | None:
    p = Path("artifacts/datasets")
    if not p.exists():
        return None
    files = sorted(p.rglob("*.parquet"), key=os.path.getmtime, reverse=True)
    if not files:
        return None
    latest = files[0]
    h = hashlib.sha1()
    # include filename and mtime to keep it fast
    h.update(str(latest).encode())
    h.update(str(os.path.getmtime(latest)).encode())
    return h.hexdigest()


@sensor(job=feast_etl_job)
def dataset_snapshot_sensor(context):
    """Sensor: triggers `feast_etl_job` when new or changed files appear under `artifacts/datasets/`.

    Emits a RunRequest per changed file with a per-file run_key for idempotency.
    State file stores known file -> hash mapping.
    """
    state = _load_state()
    known = state.get("known", {}) if isinstance(state, dict) else {}
    p = Path("artifacts/datasets")
    if not p.exists():
        return

    # build current map of file -> hash
    current = {}
    for f in sorted(p.rglob("*.parquet")):
        key = f.as_posix()
        try:
            mtime = os.path.getmtime(f)
        except OSError:
            continue
        h = hashlib.sha1((key + str(mtime)).encode()).hexdigest()
        current[key] = h

    # remove known entries that no longer exist
    removed = [k for k in known.keys() if k not in current]
    if removed:
        for k in removed:
            known.pop(k, None)

    # detect new or changed files and yield one RunRequest per change
    for key, h in current.items():
        if known.get(key) != h:
            known[key] = h
            # persist state atomically
            _save_state({"known": known, "ts": datetime.utcnow().isoformat()})
            context.log.info(f"dataset snapshot changed: {key}")
            # use run_key combining path and hash to avoid collisions
            run_key = hashlib.sha1((key + h).encode()).hexdigest()
            yield RunRequest(run_key=run_key, run_config={"resources": {"snapshot_path": key}})
    return


daily_schedule = ScheduleDefinition(job=feast_etl_job, cron_schedule="0 2 * * *")


@repository
def octa_repo():
    return [feast_etl_job, daily_schedule, dataset_snapshot_sensor]
