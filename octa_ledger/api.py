from __future__ import annotations

from typing import Any, Dict, Optional

from .events import AuditEvent
from .store import LedgerStore


class TradingBlocked(Exception):
    pass


class LedgerAPI:
    def __init__(self, path: str, signing_key: Optional[bytes] = None):
        self.store = LedgerStore(path, signing_key_bytes=signing_key)

    def audit_or_fail(
        self, actor: str, action: str, payload: Dict[str, Any], severity: str = "INFO"
    ) -> None:
        ev = AuditEvent.create(
            actor=actor, action=action, payload=payload, severity=severity
        )
        try:
            self.store.append(ev)
        except Exception as e:
            # Any failure to persist audit logs must block trading
            raise TradingBlocked(f"TRADING_BLOCKED: audit failure: {e}") from e

    # query helpers
    def last_n(self, n: int):
        return self.store.last_n(n)

    def by_action(self, action: str):
        return self.store.by_action(action)

    def by_time_range(self, start_ts: str, end_ts: str):
        return self.store.by_time_range(start_ts, end_ts)
