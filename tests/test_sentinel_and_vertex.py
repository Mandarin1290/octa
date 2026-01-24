from octa_core.ids import generate_id
from octa_core.types import Identifier
from octa_sentinel.core import Sentinel
from octa_vertex.orders import ExecutionEngine, Order, OrderStatus, RiskAwareEngine


class DummyEngine(ExecutionEngine):
    def execute(self, order: Order) -> Order:
        order.status = OrderStatus.FILLED
        return order


def test_risk_blocks_when_rule_fails(monkeypatch) -> None:
    sent = Sentinel.get_instance()
    sent.set_enabled(True)

    class BlockAllRule:
        def evaluate(self, ctx: dict) -> bool:
            return False

    sent._rules = [BlockAllRule()]

    engine = RiskAwareEngine(DummyEngine())
    o = Order(id=Identifier(str(generate_id())), symbol="XYZ", qty=1.0)
    try:
        engine.execute(o)
        blocked = False
    except Exception:
        blocked = True
    assert blocked
