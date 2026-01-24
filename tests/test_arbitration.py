from decimal import Decimal

from octa_alpha.arbitration import AlphaProfile, detect_overlaps, resolve_arbitration


def test_redundant_alpha_defunded():
    total = Decimal("1000")
    # two identical exposures -> high overlap
    exp = [Decimal("1"), Decimal("0"), Decimal("0")]
    a = AlphaProfile(
        alpha_id="A",
        requested_capital=Decimal("1000"),
        expected_return=Decimal("0.05"),
        volatility=Decimal("0.02"),
        base_confidence=Decimal("1.0"),
        exposure=exp,
    )
    b = AlphaProfile(
        alpha_id="B",
        requested_capital=Decimal("1000"),
        expected_return=Decimal("0.05"),
        volatility=Decimal("0.02"),
        base_confidence=Decimal("1.0"),
        exposure=exp,
    )
    allocs = resolve_arbitration([a, b], total, overlap_threshold=Decimal("0.9"))
    # one should receive full allocation (or as much as possible), the duplicate should be defunded
    allocated = {r["alpha_id"]: r["allocated_capital"] for r in allocs}
    assert (
        allocated["A"] == Decimal("1000.00000000")
        and allocated["B"] == Decimal("0.00000000")
    ) or (
        allocated["B"] == Decimal("1000.00000000")
        and allocated["A"] == Decimal("0.00000000")
    )


def test_diversification_preserved():
    total = Decimal("1000")
    # orthogonal exposures -> low overlap, both should get allocations
    exp1 = [Decimal("1"), Decimal("0"), Decimal("0")]
    exp2 = [Decimal("0"), Decimal("1"), Decimal("0")]
    a = AlphaProfile(
        alpha_id="A",
        requested_capital=Decimal("600"),
        expected_return=Decimal("0.05"),
        volatility=Decimal("0.02"),
        base_confidence=Decimal("1.0"),
        exposure=exp1,
    )
    b = AlphaProfile(
        alpha_id="B",
        requested_capital=Decimal("600"),
        expected_return=Decimal("0.04"),
        volatility=Decimal("0.02"),
        base_confidence=Decimal("1.0"),
        exposure=exp2,
    )
    allocs = resolve_arbitration([a, b], total, overlap_threshold=Decimal("0.9"))
    allocated = {r["alpha_id"]: r["allocated_capital"] for r in allocs}
    assert allocated["A"] > Decimal("0")
    assert allocated["B"] > Decimal("0")


def test_detect_overlaps_symmetry():
    exp1 = [Decimal("1"), Decimal("0")]
    exp2 = [Decimal("0"), Decimal("1")]
    a = AlphaProfile(
        alpha_id="A",
        requested_capital=Decimal("1"),
        expected_return=Decimal("0"),
        volatility=Decimal("1"),
        base_confidence=Decimal("1"),
        exposure=exp1,
    )
    b = AlphaProfile(
        alpha_id="B",
        requested_capital=Decimal("1"),
        expected_return=Decimal("0"),
        volatility=Decimal("1"),
        base_confidence=Decimal("1"),
        exposure=exp2,
    )
    overlaps = detect_overlaps([a, b])
    assert overlaps["A"]["B"] == overlaps["B"]["A"]
