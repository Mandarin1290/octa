from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(obj: Any) -> str:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False
    )


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class AuditEntry:
    index: int
    timestamp: str
    user: str
    resource: str
    action: str
    query: Dict
    result_digest: str
    prev_hash: Optional[str]
    entry_hash: str

    def to_dict(self) -> Dict:
        return asdict(self)


class AuditInterface:
    """Read-only external audit interface.

    - Returns only evidence (hashes / snapshots), never execution handles.
    - Records every access in an append-only chained log (hash chain).
    - Provides `verify_logs()` to confirm tamper-resistance of in-memory log.
    """

    def __init__(self, registry=None, governance=None, accounting=None, models=None):
        # subsystems are referenced but only inspected through safe snapshot methods
        self._registry = registry
        self._governance = governance
        self._accounting = accounting
        self._models = models
        self._log: List[AuditEntry] = []

    def _digest_result(self, result: Any) -> str:
        try:
            canonical = _canonical(result)
        except Exception:
            canonical = _canonical(str(result))
        return _sha256(canonical)

    def _append_log(
        self, user: str, resource: str, action: str, query: Dict, result: Any
    ) -> AuditEntry:
        prev = self._log[-1].entry_hash if self._log else None
        idx = len(self._log)
        ts = _now_iso()
        res_digest = self._digest_result(result)
        payload = {
            "index": idx,
            "timestamp": ts,
            "user": user,
            "resource": resource,
            "action": action,
            "query": query,
            "result_digest": res_digest,
            "prev_hash": prev,
        }
        entry_hash = _sha256(_canonical(payload))
        entry = AuditEntry(
            index=idx,
            timestamp=ts,
            user=user,
            resource=resource,
            action=action,
            query=query,
            result_digest=res_digest,
            prev_hash=prev,
            entry_hash=entry_hash,
        )
        self._log.append(entry)
        return entry

    def access_registry_snapshot(self, user: str, query: Dict | None = None) -> Dict:
        """Return a defensive snapshot of registry evidence: list of asset@version -> evidence_hash and lifecycle.

        `query` may be used to filter but will not allow execution beyond reading evidence.
        """
        query = query or {}
        # Build snapshot
        snapshot: Any = {}
        if self._registry is not None:
            for aid in list(self._registry.list_assets()):
                asset = self._registry.get_asset(aid)
                for ver, v in asset.versions.items():
                    node = f"{aid}@{ver}"
                    snapshot[node] = {
                        "evidence_hash": v.evidence_hash,
                        "lifecycle": v.lifecycle,
                        "created_at": v.created_at,
                    }

        # append log entry
        self._append_log(
            user=user,
            resource="registry",
            action="snapshot",
            query=query,
            result=snapshot,
        )
        # return a deep copy via canonical round-trip to ensure callers can't mutate internal objects
        return json.loads(_canonical(snapshot))

    def access_governance_log(self, user: str, query: Dict | None = None) -> Dict:
        query = query or {}
        snapshot: Any = {}
        if self._governance is not None and hasattr(self._governance, "audit_trail"):
            # Assume audit_trail returns serializable list of records or dict
            try:
                raw = self._governance.audit_trail()
            except Exception:
                raw = str(self._governance)
            snapshot = raw

        self._append_log(
            user=user,
            resource="governance",
            action="read_audit_trail",
            query=query,
            result=snapshot,
        )
        return (
            json.loads(_canonical(snapshot))
            if not isinstance(snapshot, str)
            else {"note": snapshot}
        )

    def access_accounting_nav(self, user: str, query: Dict | None = None) -> Dict:
        query = query or {}
        snapshot: Any = {}
        if self._accounting is not None and hasattr(self._accounting, "nav_history"):
            try:
                raw = self._accounting.nav_history()
            except Exception:
                raw = str(self._accounting)
            snapshot = raw

        self._append_log(
            user=user,
            resource="accounting",
            action="nav_read",
            query=query,
            result=snapshot,
        )
        return (
            json.loads(_canonical(snapshot))
            if not isinstance(snapshot, str)
            else {"note": snapshot}
        )

    def access_model_lineage(self, user: str, query: Dict | None = None) -> Dict:
        query = query or {}
        snapshot: Any = {}
        if self._models is not None and hasattr(self._models, "list_models"):
            try:
                models = self._models.list_models()
                # produce safe summary: name -> evidence_hash / version
                for m in models:
                    # expect m to be mapping-like
                    name = (
                        m.get("name")
                        if isinstance(m, dict)
                        else getattr(m, "name", str(m))
                    )
                    ver = (
                        m.get("version")
                        if isinstance(m, dict)
                        else getattr(m, "version", "")
                    )
                    evidence = (
                        m.get("evidence_hash")
                        if isinstance(m, dict)
                        else getattr(m, "evidence_hash", "")
                    )
                    snapshot[name] = {"version": ver, "evidence_hash": evidence}
            except Exception:
                snapshot = str(self._models)

        self._append_log(
            user=user,
            resource="models",
            action="lineage_read",
            query=query,
            result=snapshot,
        )
        return (
            json.loads(_canonical(snapshot))
            if not isinstance(snapshot, str)
            else {"note": snapshot}
        )

    def list_logs(self) -> List[Dict]:
        # Return defensive copy of logs
        return [entry.to_dict() for entry in self._log]

    def verify_logs(self) -> bool:
        # Recompute the chain and verify hashes
        prev = None
        for entry in self._log:
            payload = {
                "index": entry.index,
                "timestamp": entry.timestamp,
                "user": entry.user,
                "resource": entry.resource,
                "action": entry.action,
                "query": entry.query,
                "result_digest": entry.result_digest,
                "prev_hash": entry.prev_hash,
            }
            recomputed = _sha256(_canonical(payload))
            if recomputed != entry.entry_hash:
                return False
            if entry.prev_hash != prev:
                return False
            prev = entry.entry_hash
        return True
