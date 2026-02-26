"""TWS readiness probe.

Verifies broker connectivity before allowing paper/live execution.
Runs health_check() + account_snapshot() in a daemon thread with a
configurable timeout so a hung network call never stalls the runner.
"""

from __future__ import annotations

import threading
from typing import Any, Dict, Protocol, runtime_checkable


@runtime_checkable
class _Probeable(Protocol):
    def health_check(self) -> Dict[str, Any]: ...
    def account_snapshot(self) -> Dict[str, Any]: ...


def tws_probe(broker: _Probeable, *, timeout_seconds: int = 10) -> bool:
    """Return True iff broker is reachable and reports ok status.

    Steps:
    1. Call ``broker.health_check()`` — must return ``{"ok": True, ...}``.
    2. Call ``broker.account_snapshot()`` — must return a dict.

    Both calls run inside a daemon thread bounded by ``timeout_seconds``.
    Any exception, unhealthy status, or timeout → returns False (fail-closed).

    Parameters
    ----------
    broker:
        Any object exposing ``health_check()`` and ``account_snapshot()``.
    timeout_seconds:
        Wall-clock budget for the combined probe.  The thread is a daemon
        so it does not block process shutdown if it outlives the timeout.
    """
    result: list[bool] = [False]

    def _probe() -> None:
        try:
            health = broker.health_check()
            if not health.get("ok"):
                return
            snapshot = broker.account_snapshot()
            if isinstance(snapshot, dict):
                result[0] = True
        except Exception:
            pass

    thread = threading.Thread(target=_probe, daemon=True, name="tws_probe")
    thread.start()
    thread.join(timeout=float(timeout_seconds))
    return result[0]
