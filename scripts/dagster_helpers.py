from __future__ import annotations

import time
from typing import Any, Callable

from scripts.retry_policy import DEFAULT_POLICY


def retry_call(fn: Callable, args: tuple | None = None, kwargs: dict | None = None, retries: int | None = None, delay: float | None = None, backoff: float | None = None) -> Any:
    args = args or ()
    kwargs = kwargs or {}
    # use provided overrides or fall back to centralized DEFAULT_POLICY
    retries = int(retries if retries is not None else DEFAULT_POLICY.retries)
    delay = float(delay if delay is not None else DEFAULT_POLICY.delay)
    backoff = float(backoff if backoff is not None else DEFAULT_POLICY.backoff)
    attempt = 0
    wait = delay
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception:
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(wait)
            wait *= backoff
