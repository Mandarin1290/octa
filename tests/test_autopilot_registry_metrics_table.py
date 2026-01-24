import json


def test_autopilot_registry_metrics_table(tmp_path):
    from octa_ops.autopilot.registry import ArtifactRegistry

    reg = ArtifactRegistry(root=str(tmp_path))
    mid = reg.add_metrics(
        run_id="r1",
        symbol="AAPL",
        timeframe="1D",
        stage="train",
        metrics_json=json.dumps({"sharpe": 1.23}),
        gate_json=json.dumps({"passed": True}),
    )
    assert isinstance(mid, int)
    assert mid > 0
