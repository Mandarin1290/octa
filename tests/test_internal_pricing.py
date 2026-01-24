from octa_capital.internal_pricing import InternalPricing
from octa_ledger.core import AuditChain


def test_capital_charge_applied_and_penalty():
    ledger = AuditChain()
    pricing = InternalPricing(
        hurdle_rate=0.10,
        penalty_multiplier=2.0,
        audit_fn=lambda e, p: ledger.append({"event": e, **p}),
    )

    gross = {"S1": 50000.0, "S2": 20000.0}
    capital = {"S1": 1_000_000.0, "S2": 100_000.0}

    res = pricing.apply_charges(gross, capital, period_days=365)

    # S1: charge = 1_000_000 * 0.1 = 100000 -> penalty applies (net negative) -> doubled charge
    s1 = res["S1"]
    assert s1.capital_charge == 100000.0 * 2.0
    assert s1.net_return == 50000.0 - s1.capital_charge

    # S2: charge = 100_000 * 0.1 = 10000 -> net positive -> no penalty
    s2 = res["S2"]
    assert s2.capital_charge == 10000.0
    assert s2.net_return == 20000.0 - 10000.0
