from decimal import Decimal

from octa_alpha.capital_rotation import RotationEngine


def test_rotation_smooth():
    total = Decimal("1000")
    current = {"A": Decimal("500"), "B": Decimal("500")}
    # target wants A to 800, B to 200 (weights proportional)
    targets = {"A": Decimal("0.8"), "B": Decimal("0.2")}
    engine = RotationEngine(
        transaction_cost_rate=Decimal("0.0"),
        max_shift_fraction=Decimal("0.2"),
        cooldown_periods=0,
    )
    new_allocs, moved, cost = engine.rotate_once(current, targets, total, period=1)
    # max_shift_fraction=0.2 -> max movable = 200. So A increases by 200 to 700, B decreases to 300
    assert new_allocs["A"] == Decimal("700.00000000")
    assert new_allocs["B"] == Decimal("300.00000000")
    assert cost == Decimal("0.00000000")


def test_costs_respected():
    total = Decimal("1000")
    current = {"A": Decimal("500"), "B": Decimal("500")}
    targets = {"A": Decimal("0.8"), "B": Decimal("0.2")}
    engine = RotationEngine(
        transaction_cost_rate=Decimal("0.01"),
        max_shift_fraction=Decimal("0.2"),
        cooldown_periods=0,
    )
    new_allocs, moved, cost = engine.rotate_once(current, targets, total, period=1)
    # cost should be > 0 and reduce net received by A
    assert cost > Decimal("0")
    # moved into A should be less than gross moved budget (200) because of costs
    assert moved["A"] < Decimal("200")
    # total capital after rotation equals initial minus cost (some rounding accepted)
    total_after = sum(new_allocs.values())
    assert total_after + cost == Decimal("1000.00000000")
