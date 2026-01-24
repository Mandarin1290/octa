import json
import os

from octa_atlas.registry import AtlasRegistry
from octa_nexus.paper_runtime import PaperRuntime


def make_manifest(tmp_path, asset_id, symbol):
    p = tmp_path / f"{asset_id}.yaml"
    data = {
        "asset_id": asset_id,
        "symbol": symbol,
        "asset_class": "EQUITY",
        "venue": "X",
        "currency": "USD",
        "parquet_path": "/tmp/none",
        "ca_provided": True,
    }
    p.write_text(json.dumps(data))
    return str(p)


def test_full_pipeline_runs(tmp_path):
    manifests = tmp_path / "manifests"
    atlas = tmp_path / "atlas"
    ledger = tmp_path / "ledger"
    vertex = tmp_path / "vertex"
    manifests.mkdir()
    atlas.mkdir()
    ledger.mkdir()
    vertex.mkdir()

    # make sample manifest
    make_manifest(manifests, "a1", "AAA")

    # prepare atlas: save a dummy artifact
    reg = AtlasRegistry(str(atlas))
    meta = type("M", (), {"to_dict": lambda self: {"author": "t"}})()
    reg.save_artifact("a1", "model", "v1", {"model": "dummy"}, meta)

    rt = PaperRuntime(
        manifests_dir=str(manifests),
        atlas_root=str(atlas),
        ledger_dir=str(ledger),
        vertex_store=str(vertex),
    )
    res = rt.run_once()
    assert isinstance(res, dict)
    assert "inference" in res
    # audit events should be present
    assert res["audit_events"] >= 1


def test_sentinel_blocks_trades(tmp_path):
    # craft a policy that blocks always (operational audit failure level triggers freeze)
    from octa_sentinel.policies import SentinelPolicy

    manifests = tmp_path / "manifests"
    atlas = tmp_path / "atlas"
    ledger = tmp_path / "ledger"
    vertex = tmp_path / "vertex"
    manifests.mkdir()
    atlas.mkdir()
    ledger.mkdir()
    vertex.mkdir()
    make_manifest(manifests, "a2", "ZZZ")
    reg = AtlasRegistry(str(atlas))
    meta = type("M", (), {"to_dict": lambda self: {"author": "t"}})()
    reg.save_artifact("a2", "model", "v1", {"model": "dummy"}, meta)

    policy = SentinelPolicy(schema_version=1, name="blocker")
    # create a sentinel backed by a ledger store with missing log to simulate audit failure
    from octa_ledger.store import LedgerStore

    missing_dir = os.path.join(str(tmp_path), "missing_ledger")
    ls = LedgerStore(missing_dir)
    se = None
    from octa_sentinel.engine import SentinelEngine

    se = SentinelEngine(
        policy=policy,
        state_path=os.path.join(str(tmp_path), "state.json"),
        ledger_store=ls,
    )
    rt = PaperRuntime(
        manifests_dir=str(manifests),
        atlas_root=str(atlas),
        ledger_dir=str(ledger),
        vertex_store=str(vertex),
        policy=policy,
        audit_enabled=False,
        sentinel_engine=se,
    )
    res = rt.run_once()
    assert res.get("blocked", False) is True


def test_audit_trace_contains_steps(tmp_path):
    manifests = tmp_path / "manifests"
    atlas = tmp_path / "atlas"
    ledger = tmp_path / "ledger"
    vertex = tmp_path / "vertex"
    manifests.mkdir()
    atlas.mkdir()
    ledger.mkdir()
    vertex.mkdir()
    make_manifest(manifests, "a3", "ABC")
    reg = AtlasRegistry(str(atlas))
    meta = type("M", (), {"to_dict": lambda self: {"author": "t"}})()
    reg.save_artifact("a3", "model", "v1", {"model": "dummy"}, meta)

    rt = PaperRuntime(
        manifests_dir=str(manifests),
        atlas_root=str(atlas),
        ledger_dir=str(ledger),
        vertex_store=str(vertex),
    )
    rt.run_once()
    # read ledger events
    from octa_ledger.store import LedgerStore

    ls = LedgerStore(str(ledger))
    events = list(ls.iter_events())
    assert any(
        e.get("action") == "inference" or e.get("action") == "load_artifact"
        for e in events
    )
