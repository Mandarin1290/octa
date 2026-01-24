from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict

try:
    from ulid import ULID  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    ULID = None  # type: ignore

import uuid

from octa_fabric.fingerprint import canonicalize


def _new_event_id() -> str:
    """Generate a robust event id.

    Supports multiple common ULID library variants and falls back to UUID.
    """

    # Prefer ulid.new() if available (common API)
    try:  # pragma: no cover - optional dependency
        import ulid as _ulid_mod  # type: ignore

        new_fn = getattr(_ulid_mod, "new", None)
        if callable(new_fn):
            return str(new_fn())
    except Exception:
        pass

    # Next, try ULID() constructor if imported and callable without args.
    try:
        if ULID is not None:
            return str(ULID())
    except TypeError:
        # Some ULID classes require a buffer; treat as unsupported.
        pass
    except Exception:
        pass

    # Fallback: UUID4 hex
    return uuid.uuid4().hex


@dataclass
class AuditEvent:
    schema_version: int
    event_id: str
    timestamp: str
    actor: str
    action: str
    payload: Dict[str, Any]
    severity: str
    prev_hash: str | None = None
    curr_hash: str | None = None

    @staticmethod
    def create(
        actor: str,
        action: str,
        payload: Dict[str, Any],
        severity: str = "INFO",
        schema_version: int = 1,
        prev_hash: str | None = None,
    ) -> "AuditEvent":
        eid = _new_event_id()
        ts = datetime.now(timezone.utc).isoformat()
        ev = AuditEvent(
            schema_version=schema_version,
            event_id=eid,
            timestamp=ts,
            actor=actor,
            action=action,
            payload=payload,
            severity=severity,
            prev_hash=prev_hash,
            curr_hash=None,
        )
        return ev

    def to_dict(self) -> Dict[str, Any]:
        # use canonical order for fingerprinting
        return asdict(self)

    def canonical(self) -> str:
        return canonicalize(self.to_dict())
