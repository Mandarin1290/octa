from decimal import Decimal

from octa_alpha.alpha_portfolio import AlphaCandidate, optimize_weights


def test_dominance_prevented():
    # one alpha with very large utility but max_per_alpha=0.3 prevents dominance
    a = AlphaCandidate(
        alpha_id="A",
        base_utility=Decimal("1000"),
        volatility=Decimal("0.05"),
        exposure=[Decimal("1"), Decimal("0")],
    )
    b = AlphaCandidate(
        alpha_id="B",
        base_utility=Decimal("1"),
        volatility=Decimal("0.05"),
        exposure=[Decimal("0"), Decimal("1")],
    )
    weights = optimize_weights(
        [a, b], max_per_alpha=Decimal("0.3"), total_risk_budget=Decimal("1.0")
    )
    assert weights["A"] <= Decimal("0.30000000")


def test_constraints_respected():
    # three similar alphas should be capped by behavior share
    a = AlphaCandidate(
        alpha_id="A",
        base_utility=Decimal("1"),
        volatility=Decimal("0.02"),
        exposure=[Decimal("1"), Decimal("0")],
    )
    b = AlphaCandidate(
        alpha_id="B",
        base_utility=Decimal("1"),
        volatility=Decimal("0.02"),
        exposure=[Decimal("1"), Decimal("0")],
    )
    c = AlphaCandidate(
        alpha_id="C",
        base_utility=Decimal("1"),
        volatility=Decimal("0.02"),
        exposure=[Decimal("0"), Decimal("1")],
    )
    weights = optimize_weights(
        [a, b, c],
        behavior_threshold=Decimal("0.9"),
        max_behavior_share=Decimal("0.6"),
        total_risk_budget=Decimal("1.0"),
    )
    cluster_share = weights["A"] + weights["B"]
    assert cluster_share <= Decimal("0.60000001")
