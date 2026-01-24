from decimal import Decimal

from octa_alpha.crowding import (
    CrowdingProfile,
    apply_crowding_penalties,
    crowding_index,
)


def test_crowded_alpha_penalized():
    # three very similar alphas -> high crowding index
    exp = [Decimal("1"), Decimal("0"), Decimal("0")]
    p1 = CrowdingProfile(alpha_id="A", exposure=exp)
    p2 = CrowdingProfile(alpha_id="B", exposure=exp)
    p3 = CrowdingProfile(alpha_id="C", exposure=exp)
    base = {"A": Decimal("1.0"), "B": Decimal("1.0"), "C": Decimal("1.0")}
    adjusted, mults = apply_crowding_penalties(
        base, [p1, p2, p3], threshold=Decimal("0.5"), exponent=Decimal("2.0")
    )
    # multipliers should be << 1
    assert all(mults[a] < Decimal("0.5") for a in ("A", "B", "C"))
    assert all(adjusted[a] < base[a] for a in ("A", "B", "C"))


def test_unique_alpha_protected():
    # two identical plus one unique
    exp_common = [Decimal("1"), Decimal("0"), Decimal("0")]
    exp_unique = [Decimal("0"), Decimal("1"), Decimal("0")]
    p1 = CrowdingProfile(alpha_id="A", exposure=exp_common)
    p2 = CrowdingProfile(alpha_id="B", exposure=exp_common)
    p3 = CrowdingProfile(alpha_id="U", exposure=exp_unique)
    base = {"A": Decimal("1.0"), "B": Decimal("1.0"), "U": Decimal("1.0")}
    indices = crowding_index([p1, p2, p3])
    # unique should have lower crowding index
    assert indices["U"] < indices["A"]
    adjusted, mults = apply_crowding_penalties(
        base, [p1, p2, p3], threshold=Decimal("0.5"), exponent=Decimal("2.0")
    )
    assert adjusted["U"] >= adjusted["A"]
