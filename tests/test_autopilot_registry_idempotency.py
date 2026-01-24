from __future__ import annotations

from octa_ops.autopilot.registry import ArtifactRegistry


def test_order_idempotency(tmp_path):
    reg = ArtifactRegistry(root=str(tmp_path))
    ok1 = reg.try_reserve_order(
        run_id="r1",
        order_key="k1",
        client_order_id="c1",
        symbol="AAA",
        timeframe="1D",
        model_id="m1",
        side="BUY",
        qty=1.0,
    )
    ok2 = reg.try_reserve_order(
        run_id="r1",
        order_key="k1",
        client_order_id="c1",
        symbol="AAA",
        timeframe="1D",
        model_id="m1",
        side="BUY",
        qty=1.0,
    )
    assert ok1 is True
    assert ok2 is False
