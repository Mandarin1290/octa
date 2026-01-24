from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class RetryPolicy:
    retries: int = 3
    delay: float = 5.0
    backoff: float = 2.0


def load_policy_from_env() -> RetryPolicy:
    try:
        r = int(os.getenv("OCTA_RETRY_RETRIES", "3"))
    except Exception:
        r = 3
    try:
        d = float(os.getenv("OCTA_RETRY_DELAY", "5.0"))
    except Exception:
        d = 5.0
    try:
        b = float(os.getenv("OCTA_RETRY_BACKOFF", "2.0"))
    except Exception:
        b = 2.0
    return RetryPolicy(retries=r, delay=d, backoff=b)


DEFAULT_POLICY = load_policy_from_env()
