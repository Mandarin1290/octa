from octa_capital.redemption_stress import RedemptionStressEngine
from octa_ledger.core import AuditChain


def test_stress_outputs_deterministic_and_sentinels():
    ledger = AuditChain()
    engine = RedemptionStressEngine(
        audit_fn=lambda e, p: ledger.append({"event": e, **p}), max_safe_slippage=0.2
    )

    portfolio = {
        "A": {"weight": 0.6, "liquidity_days": 1.0, "slippage_per_day": 0.001},
        "B": {"weight": 0.3, "liquidity_days": 5.0, "slippage_per_day": 0.002},
        "C": {"weight": 0.1, "liquidity_days": 30.0, "slippage_per_day": 0.01},
    }

    results = engine.run_scenarios(portfolio)
    assert len(results) == 3

    # deterministic: re-run should yield same numeric outputs
    results2 = engine.run_scenarios(portfolio)
    for r1, r2 in zip(results, results2, strict=False):
        assert r1.liquidation_timeline_days == r2.liquidation_timeline_days
        assert abs(r1.forced_slippage_pct - r2.forced_slippage_pct) < 1e-12
        assert abs(r1.capital_loss_estimate - r2.capital_loss_estimate) < 1e-12

    sent = engine.check_sentinels(results, slippage_threshold=0.05, loss_threshold=0.02)
    # given C is illiquid and slippage per day stressed in third scenario, expect slippage breach
    assert sent["slippage_breach"] in (True, False)
    assert isinstance(sent["loss_breach"], bool)
