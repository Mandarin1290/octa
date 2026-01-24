from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(obj: Any) -> str:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False
    )


def _sha256_obj(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


@dataclass
class FinalCertification:
    completion_hash: str
    system_fingerprint: Dict[str, Any]
    certified_scope: List[str]
    certified_at: str
    cert_hash: str
    frozen: bool = True
    reopen_requests: List[Dict[str, Any]] = field(default_factory=list)

    def to_manifest(self) -> Dict[str, Any]:
        return asdict(self)


class FinalCertManager:
    """Generates final certification, freezes core and records governance reopen requests.

    Rules:
    - Certification freezes the system: `frozen=True` and change attempts should be disallowed.
    - Reopen requires governance approval which is recorded as `reopen_requests`.
    """

    def __init__(self, provenance: Dict[str, Any]):
        # provenance: evidence fingerprints from subsystems
        self.provenance = provenance
        self._cert: Optional[FinalCertification] = None

    def certify(
        self, scope: List[str], notes: Optional[str] = None
    ) -> FinalCertification:
        if self._cert and self._cert.frozen:
            raise RuntimeError("system already certified and frozen")
        completion_hash = _sha256_obj(self.provenance)
        system_fingerprint = {
            k: (
                v.get("evidence_hash")
                if isinstance(v, dict) and "evidence_hash" in v
                else str(v)
            )
            for k, v in self.provenance.items()
        }
        certified_at = _now_iso()
        manifest = {
            "completion_hash": completion_hash,
            "fingerprint": system_fingerprint,
            "scope": sorted(scope),
            "certified_at": certified_at,
            "notes": notes,
        }
        cert_hash = _sha256_obj(manifest)
        self._cert = FinalCertification(
            completion_hash=completion_hash,
            system_fingerprint=system_fingerprint,
            certified_scope=sorted(scope),
            certified_at=certified_at,
            cert_hash=cert_hash,
            frozen=True,
            reopen_requests=[],
        )
        return self._cert

    def get_cert(self) -> Optional[FinalCertification]:
        return self._cert

    def request_reopen(self, requester: str, reason: str) -> Dict[str, Any]:
        if not self._cert:
            raise RuntimeError("no certification exists")
        req = {
            "requester": requester,
            "reason": reason,
            "requested_at": _now_iso(),
            "status": "pending",
        }
        self._cert.reopen_requests.append(req)
        return req

    def approve_reopen(self, index: int, approver: str) -> Dict[str, Any]:
        if not self._cert:
            raise RuntimeError("no certification exists")
        try:
            req = self._cert.reopen_requests[index]
        except Exception:
            raise IndexError("unknown reopen request") from None
        req["status"] = "approved"
        req["approved_by"] = approver
        req["approved_at"] = _now_iso()
        # flip freeze to allow changes; governance must ensure follow-up
        self._cert.frozen = False
        return req

    def reject_reopen(self, index: int, approver: str) -> Dict[str, Any]:
        if not self._cert:
            raise RuntimeError("no certification exists")
        try:
            req = self._cert.reopen_requests[index]
        except Exception:
            raise IndexError("unknown reopen request") from None
        req["status"] = "rejected"
        req["rejected_by"] = approver
        req["rejected_at"] = _now_iso()
        return req

    def is_frozen(self) -> bool:
        return bool(self._cert and self._cert.frozen)
