from octa_atlas.registry import AtlasRegistry
from octa_ledger.store import LedgerStore
from octa_sentinel.stress_harness import Position, StressHarness


def test_historical_deterministic_and_artifact(tmp_path):
    lp = tmp_path / "ledger"
    ap = tmp_path / "atlas"
    ls = LedgerStore(str(lp))
    reg = AtlasRegistry(str(ap))

    harness = StressHarness(ls, reg)
    positions = [
        Position(symbol="AAA", notional=1000.0, price=10.0, asset_class="EQUITY"),
        Position(symbol="BBB", notional=2000.0, price=5.0, asset_class="EQUITY"),
    ]
    returns = {"AAA": [0.01, -0.02], "BBB": [0.005, 0.0]}
    r1 = harness.run_historical(
        "pf1", positions, returns, window_name="w1", version="v1"
    )
    r2 = harness.run_historical(
        "pf1", positions, returns, window_name="w1", version="v1"
    )
    assert r1 == r2

    # artifact stored in atlas
    obj, meta = reg.load_latest("pf1", "risk_profile")
    assert obj.profile["total_pnl"] == r1["total_pnl"]


def test_parametric_outputs_and_provenance(tmp_path):
    lp = tmp_path / "ledger2"
    ap = tmp_path / "atlas2"
    ls = LedgerStore(str(lp))
    reg = AtlasRegistry(str(ap))

    harness = StressHarness(ls, reg)
    positions = [
        Position(symbol="EQ", notional=10000.0, price=100.0, asset_class="EQUITY"),
        Position(symbol="FX", notional=5000.0, price=1.0, asset_class="FX"),
    ]
    shocks = {"EQUITY": -0.2, "FX": -0.05, "correlation_to_one": True}
    res = harness.run_parametric("pf2", positions, shocks, version="p1")
    # deterministic structure
    assert "per_asset" in res
    # artifact present
    obj, meta = reg.load_latest("pf2", "risk_profile")
    assert meta["artifact_hash"]
