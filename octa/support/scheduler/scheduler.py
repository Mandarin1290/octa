from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

from octa.support.scheduler.daily_altdata_job import run_daily_altdata_refresh


def next_run_utc(now: datetime, schedule: str) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    hour_str, minute_str = schedule.split(":")
    hour = int(hour_str)
    minute = int(minute_str)
    run_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if run_at <= now:
        run_at = run_at + timedelta(days=1)
    return run_at


def run_loop(
    *,
    schedule: str,
    oneshot: bool = False,
    now_fn: Callable[[], datetime] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> None:
    now_fn = now_fn or (lambda: datetime.now(timezone.utc))
    sleep_fn = sleep_fn or (lambda seconds: None)
    while True:
        now = now_fn()
        run_at = next_run_utc(now, schedule)
        delay = (run_at - now).total_seconds()
        if delay > 0:
            sleep_fn(delay)
        run_daily_altdata_refresh(now=run_at, symbols=[])
        if oneshot:
            break


__all__ = ["next_run_utc", "run_loop", "run_daily_altdata_refresh"]
