from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from octa.support.scheduler.daily_altdata_refresh import run_daily_altdata_refresh


def _next_run(now: datetime, hour: int, minute: int) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    run_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if run_at <= now:
        run_at = run_at + timedelta(days=1)
    return run_at


def _sleep_seconds(seconds: float) -> None:
    return None


def run_daemon(
    *,
    by_hour: int,
    by_minute: int,
    lock_root: str | None = None,
    max_loops: int | None = None,
    now_fn: Callable[[], datetime] | None = None,
) -> None:
    now_fn = now_fn or (lambda: datetime.now(timezone.utc))
    root = Path(lock_root) if lock_root else Path("octa") / "var" / "locks"
    root.mkdir(parents=True, exist_ok=True)
    loops = 0
    while True:
        now = now_fn()
        run_at = _next_run(now, by_hour, by_minute)
        lock_path = root / f"altdata_refresh_{run_at.date().isoformat()}.lock"
        if lock_path.exists():
            _sleep_seconds(0.0)
        else:
            lock_path.write_text(run_at.isoformat(), encoding="utf-8")
            try:
                run_daily_altdata_refresh(schedule_ts_utc=run_at)
            finally:
                try:
                    lock_path.unlink()
                except Exception:
                    pass
        loops += 1
        if max_loops is not None and loops >= max_loops:
            break


__all__ = ["run_daemon", "run_daily_altdata_refresh", "_sleep_seconds", "_next_run"]
