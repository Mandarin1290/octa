from __future__ import annotations

from octa.core.cascade.adapters import SafeNoopGate
from octa.core.cascade.registry import build_default_gate_stack


def test_build_default_gate_stack_order_and_fallback() -> None:
    stack = build_default_gate_stack()

    assert len(stack) == 5
    assert [gate.timeframe for gate in stack] == ["1D", "30M", "1H", "5M", "1M"]

    assert not isinstance(stack[0], SafeNoopGate)
    assert not isinstance(stack[1], SafeNoopGate)
    assert not isinstance(stack[2], SafeNoopGate)
    assert not isinstance(stack[3], SafeNoopGate)
    assert not isinstance(stack[4], SafeNoopGate)
