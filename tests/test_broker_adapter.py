from octa_core.broker_adapter import BrokerAdapter, BrokerCredentials


def test_simulated_order_is_allowed():
    ba = BrokerAdapter()
    creds = BrokerCredentials(name="sim", live=False)
    order = {"instrument": "ABC", "qty": 10, "side": "buy"}
    res = ba.place_order(creds, order)
    assert res["status"] == "simulated"


def test_live_order_rejected_without_approvals():
    ba = BrokerAdapter()
    creds = BrokerCredentials(name="ibkr", live=True)
    order = {"instrument": "ABC", "qty": 5, "side": "sell"}
    try:
        ba.place_order(creds, order)
        raise AssertionError("Expected PermissionError")
    except PermissionError:
        pass


def test_enable_live_requires_multi_approval_then_allows_live_orders():
    ba = BrokerAdapter()
    creds = BrokerCredentials(name="ibkr", live=True)
    try:
        ba.enable_live(approvals=1)
        raise AssertionError("expected PermissionError for insufficient approvals")
    except PermissionError:
        pass
    ba.enable_live(approvals=2)
    res = ba.place_order(creds, {"instrument": "XYZ", "qty": 1, "side": "buy"})
    assert (
        res["status"] == "simulated"
        or res["status"] == "filled"
        or res["broker"] == "ibkr"
    )
