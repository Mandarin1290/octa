from octa_vertex.pretrade_regulatory import PreTradeRegulator


class SentinelMock:
    def __init__(self):
        self.calls = []

    def set_gate(self, level, reason):
        self.calls.append((level, reason))


class AuditStub:
    def __init__(self):
        self.events = []

    def __call__(self, e, p):
        self.events.append((e, p))


def test_naked_short_blocked():
    sentinel = SentinelMock()
    audit = AuditStub()
    cfg = {"require_locate": True}
    reg = PreTradeRegulator(config=cfg, sentinel_api=sentinel, audit_fn=audit)

    # positions lookup returns zero holdings
    def pos_lookup(acct, instr):
        return 0.0

    def loc_lookup(acct, instr):
        return False

    order = {"account_id": "A1", "instrument": "AAPL", "side": "SELL", "qty": 10}
    allowed, reason = reg.pre_trade_check(
        order, positions_lookup=pos_lookup, locates_lookup=loc_lookup
    )
    assert not allowed
    assert reason == "naked_short"
    # sentinel frozen at level 3
    assert any(c[0] == 3 and c[1].startswith("naked_short") for c in sentinel.calls)


def test_cancel_storm_detected():
    sentinel = SentinelMock()
    audit = AuditStub()
    cfg = {"cancel_threshold": 2, "cancel_window_seconds": 60}
    reg = PreTradeRegulator(config=cfg, sentinel_api=sentinel, audit_fn=audit)

    order = {"account_id": "A1", "instrument": "FUT_ES", "side": "BUY", "qty": 1}
    # simulate quick cancels
    reg.record_cancel(order)
    reg.record_cancel(order)
    reg.record_cancel(order)

    # sentinel should have been notified
    assert any(
        "cancel_storm" in c[1] or c[1].startswith("cancel_storm")
        for c in sentinel.calls
    )
