import time

from octa_wargames.execution_abuse import (
    ExchangeSimulator,
    OrderManagementSystem,
)


def test_throttling_engaged():
    oms = OrderManagementSystem(max_orders_per_sec=3, exposure_cap=1000)
    strategy = "flooder"
    accepted = 0
    rejected = 0
    for _i in range(10):
        r = oms.receive_order(strategy, symbol="AAA", qty=1, side="buy")
        if r["accepted"]:
            accepted += 1
        else:
            rejected += 1
        # tight loop to trigger throttle
    assert rejected > 0
    assert accepted <= 3


def test_exposure_capped():
    oms = OrderManagementSystem(max_orders_per_sec=100, exposure_cap=5)
    strategy = "big"
    outs = []
    for _i in range(10):
        r = oms.receive_order(strategy, symbol="AAA", qty=1, side="buy")
        outs.append(r)
    accepted = [o for o in outs if o["accepted"]]
    # exposure cap 5 => at most 5 accepted
    assert len(accepted) <= 5


def test_exchange_rejection_storm_handled():
    oms = OrderManagementSystem(max_orders_per_sec=100, exposure_cap=1000)
    exch = ExchangeSimulator(reject_rate=0.9)
    strategy = "rusty"
    accepted = 0
    rejections = 0
    for _i in range(20):
        r = oms.receive_order(strategy, "BBB", 1, "buy")
        if not r["accepted"]:
            continue
        # construct ephemeral Order object for ExchangeSimulator
        from octa_wargames.execution_abuse import Order as O

        order = O(
            id=r["order_id"],
            strategy=strategy,
            symbol="BBB",
            qty=1,
            side="buy",
            ts=time.time(),
        )
        res = exch.send_order(order)
        if res["accepted"]:
            oms.record_fill(
                order.id, strategy, order.symbol, order.qty, res["fill_price"]
            )
            accepted += 1
        else:
            rejections += 1
    # many rejections expected
    assert rejections > 0
    # OMS should have recorded only accepted fills and remain functional
    assert accepted + rejections >= 1
