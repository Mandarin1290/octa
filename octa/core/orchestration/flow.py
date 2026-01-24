from __future__ import annotations

from dagster import job, op

from .runner import run_cascade


@op(
    config_schema={
        "config_path": str,
        "resume": bool,
        "run_id": str,
        "universe_limit": int,
    }
)
def run_cascade_op(context):
    cfg = context.op_config
    run_cascade(
        config_path=cfg.get("config_path") or None,
        resume=bool(cfg.get("resume", False)),
        run_id=cfg.get("run_id") or None,
        universe_limit=int(cfg.get("universe_limit", 0)),
    )


@job
def cascade_job():
    run_cascade_op()

