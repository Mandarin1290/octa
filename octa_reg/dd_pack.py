import copy
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _canonical_serialize(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _compute_hash(content: Dict[str, Any]) -> str:
    canon = _canonical_serialize(content)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


@dataclass
class DDPack:
    id: str
    snapshot: Dict[str, Any]
    ts: str
    hash: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "snapshot": copy.deepcopy(self.snapshot),
            "ts": self.ts,
            "hash": self.hash,
        }


def discover_architecture(root: Optional[str] = None) -> List[str]:
    base = Path(root) if root else Path(__file__).resolve().parents[1]
    comps = [
        p.name for p in base.iterdir() if p.is_dir() and p.name.startswith("octa_")
    ]
    return sorted(comps)


def create_dd_pack(
    *,
    incident_manager=None,
    control_matrix=None,
    model_risk=None,
    recovery_manager=None,
    ops_dashboard=None,
    root: Optional[str] = None,
) -> DDPack:
    # gather deterministic snapshot
    snapshot: Dict[str, Any] = {}
    snapshot["system_architecture"] = discover_architecture(root)

    # risk framework: control counts per objective
    if control_matrix is not None:
        objs = sorted(control_matrix.objectives.keys())
        controls = {
            oid: [c.id for c in control_matrix.get_controls_for_objective(oid)]
            for oid in objs
        }
        snapshot["risk_framework"] = {"objectives": objs, "controls": controls}
    else:
        snapshot["risk_framework"] = {}

    # governance: model inventory summary
    if model_risk is not None:
        models = [
            {"id": m.id, "name": m.name, "version": m.version, "approved": m.approved}
            for m in model_risk.list_models()
        ]
        snapshot["governance"] = {"models": models}
    else:
        snapshot["governance"] = {}

    # incident history
    if incident_manager is not None:
        incs = incident_manager.list_incidents()
        snapshot["incidents"] = [
            {"id": i.id, "title": i.title, "severity": i.severity.name, "ts": i.ts}
            for i in incs
        ]
    else:
        snapshot["incidents"] = []

    # recovery progress
    if recovery_manager is not None:
        snapshot["recovery"] = {
            "in_recovery": bool(recovery_manager.in_recovery),
            "checkpoints": len(recovery_manager.checkpoints),
        }
    else:
        snapshot["recovery"] = {}

    # ops dashboard summary if provided
    if ops_dashboard is not None:
        snapshot["ops_snapshot"] = ops_dashboard.snapshot()

    # compute reproducible hash excluding timestamp
    content_for_hash = copy.deepcopy(snapshot)
    # deterministic ordering already enforced
    pack_hash = _compute_hash(content_for_hash)
    pid = hashlib.sha1(pack_hash.encode("utf-8")).hexdigest()
    ts = _utc_now_iso()
    return DDPack(id=pid, snapshot=snapshot, ts=ts, hash=pack_hash)


def export_dd_pack_json(pack: DDPack) -> str:
    return _canonical_serialize(pack.to_dict())


__all__ = ["DDPack", "create_dd_pack", "export_dd_pack_json", "discover_architecture"]
