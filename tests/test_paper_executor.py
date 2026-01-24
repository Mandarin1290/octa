from octa_core.ids import generate_id
from octa_core.types import Identifier
from octa_ledger.api import LedgerAPI
from octa_ledger.store import LedgerStore
from octa_vertex.models import Order, OrderSide, OrderStatus
from octa_vertex.paper_executor import PaperExecutor


def make_order(qty=100.0):
    oid = generate_id("order")
    return Order(
        id=Identifier(str(oid)),
        intent_id=str(generate_id("intent")),
        symbol="AAA",
        side=OrderSide.BUY,
        qty=qty,
        price=10.0,
        status=OrderStatus.NEW,
    )


def test_partial_fills_and_costs(tmp_path):
    ledger_dir = str(tmp_path / "ledger")
    ls = LedgerStore(ledger_dir)
    api = LedgerAPI(ledger_dir)

    order = make_order(qty=1000.0)
    # bars: small per-bar volume so participation forces partial fills
    bars = [
        {"vwap": 10.0, "volume": 50, "ts": "2025-01-01T10:00:00+00:00"},
        {"vwap": 10.1, "volume": 50, "ts": "2025-01-01T10:01:00+00:00"},
        {"vwap": 10.2, "volume": 50, "ts": "2025-01-01T10:02:00+00:00"},
    ]

    exec = PaperExecutor(participation=0.5, ledger_api=api, sentinel=None)
    reports = exec.execute(order, bars, adv=10000, sigma=0.02, half_spread=0.01)
    # should be partial fills (not fully filled)
    assert any(r.status == OrderStatus.PARTIAL for r in reports)
    # ledger should contain order_fill events
    events = list(ls.iter_events())
    assert any(ev.get("action") == "order_fill" for ev in events)


def test_sentinel_freeze_stops_execution(tmp_path):
    ledger_dir = str(tmp_path / "ledger2")
    ls = LedgerStore(ledger_dir)
    api = LedgerAPI(ledger_dir)

    order = make_order(qty=100.0)
    bars = [{"vwap": 10.0, "volume": 100, "ts": "2025-01-01T10:00:00+00:00"}]

    class FreezeSentinel:
        def evaluate(self, _):
            class D:
                level = 2
                reason = "freeze"

            return D()

    exec = PaperExecutor(participation=0.5, ledger_api=api, sentinel=FreezeSentinel())
    exec.execute(order, bars, adv=10000, sigma=0.02, half_spread=0.01)
    # no fills should have occurred
    events = list(ls.iter_events())
    assert not any(ev.get("action") == "order_fill" for ev in events)
    # but an execution_halted should be logged
    assert any(ev.get("action") == "execution_halted" for ev in events)
