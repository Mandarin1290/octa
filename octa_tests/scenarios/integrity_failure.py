from __future__ import annotations

import os
from typing import Any

from octa_atlas.registry import AtlasRegistry, FileIntegrityError
from octa_ledger.events import AuditEvent
from octa_ledger.store import LedgerStore
from octa_nexus.bus import NexusBus
from octa_nexus.messages import RiskDecision, _now_iso


def run(bus_path: str, tmpdir) -> dict[str, Any]:
    repo = os.path.join(tmpdir, "atlas")
    reg = AtlasRegistry(repo)
    asset = "a1"
    typ = "model"
    ver = "v1"
    # write a dummy artifact
    obj = {"hello": "world"}
    meta = type("M", (), {"to_dict": lambda self: {"author": "t"}})()
    reg.save_artifact(asset, typ, ver, obj, meta)

    # corrupt artifact file
    art_dir = os.path.join(repo, "models", asset, typ, ver)
    art_path = os.path.join(art_dir, "artifact.pkl")
    with open(art_path, "wb") as fh:
        fh.write(b"corrupt")

    b = NexusBus(bus_path)
    ledger_dir = os.path.join(tmpdir, "ledger")
    ls = LedgerStore(ledger_dir)

    incidents = 0
    try:
        reg.load_latest(asset, typ)
    except FileIntegrityError:
        # publish freeze and audit event
        rd = RiskDecision(
            id="rd-int",
            type="RiskDecision",
            ts=_now_iso(),
            decision="FREEZE",
            reason="artifact_integrity",
        )
        b.publish(rd)
        ev = AuditEvent.create(
            actor="atlas",
            action="artifact_load_failure",
            payload={"asset": asset, "version": ver},
            severity="ERROR",
            prev_hash=None,
        )
        ls.append(ev)
        incidents = 1

    return {"incidents": incidents, "ledger_events": len(list(ls.iter_events()))}
