from decimal import Decimal

from octa_alpha.competition import Submission, run_competition


def test_weaker_alpha_loses_allocation():
    total = Decimal("1000")
    # alpha A has stronger risk-adjusted utility than B
    a = Submission(
        alpha_id="A",
        requested_capital=Decimal("1000"),
        expected_return=Decimal("0.10"),
        volatility=Decimal("0.05"),
        base_confidence=Decimal("1.0"),
        bid_price=Decimal("0"),
    )
    b = Submission(
        alpha_id="B",
        requested_capital=Decimal("1000"),
        expected_return=Decimal("0.02"),
        volatility=Decimal("0.05"),
        base_confidence=Decimal("1.0"),
        bid_price=Decimal("0"),
    )
    allocs = run_competition([a, b], total)
    # first allocation should be to A and exhaust the pool
    assert allocs[0]["alpha_id"] == "A"
    assert allocs[0]["allocated_capital"] == total
    assert allocs[1]["allocated_capital"] == Decimal("0.00000000")


def test_competition_deterministic():
    total = Decimal("500")
    s1 = Submission(
        alpha_id="X",
        requested_capital=Decimal("300"),
        expected_return=Decimal("0.05"),
        volatility=Decimal("0.02"),
        base_confidence=Decimal("0.9"),
        bid_price=Decimal("1.0"),
    )
    s2 = Submission(
        alpha_id="Y",
        requested_capital=Decimal("300"),
        expected_return=Decimal("0.04"),
        volatility=Decimal("0.015"),
        base_confidence=Decimal("0.95"),
        bid_price=Decimal("1.0"),
    )
    a1 = run_competition([s1, s2], total)
    a2 = run_competition([s1, s2], total)
    assert a1 == a2
