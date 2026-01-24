from octa_ops.market_crisis import MarketCrisisManager


class SimplePortfolio:
    def __init__(self, exposures):
        # exposures: dict asset->numeric size
        self.exposures = dict(exposures)

    def total_exposure(self):
        return sum(self.exposures.values())

    def reduce_exposure(
        self, reduction_fraction: float, pessimistic_liquidity: bool = False
    ):
        """Reduce exposures proportionally. Return the fraction actually reduced (0..1 of total at start).

        In tests we treat reduction_fraction as fraction of exposure to remove.
        """
        if reduction_fraction <= 0:
            return 0.0
        if reduction_fraction > 1:
            reduction_fraction = 1.0
        before = self.total_exposure()
        for k in list(self.exposures.keys()):
            self.exposures[k] = self.exposures[k] * (1.0 - reduction_fraction)
        after = self.total_exposure()
        removed = before - after
        # Return applied fraction of original total
        return removed / before if before > 0 else 0.0


def test_exposure_reduced():
    p = SimplePortfolio({"A": 100.0, "B": 50.0})
    mgr = MarketCrisisManager(portfolio=p)

    initial = p.total_exposure()
    metrics = {"volatility": 0.1, "correlation": 0.1, "liquidity": 0.5}
    res = mgr.evaluate(metrics, actor="system")

    assert res["triggered"] is True
    assert res["reduction_applied"] > 0
    assert p.total_exposure() < initial


def test_kill_switch_respected_and_human_override_logged():
    class SpyPortfolio(SimplePortfolio):
        def __init__(self, exposures):
            super().__init__(exposures)
            self.reduce_calls = 0

        def reduce_exposure(
            self, reduction_fraction: float, pessimistic_liquidity: bool = False
        ):
            self.reduce_calls += 1
            return super().reduce_exposure(reduction_fraction, pessimistic_liquidity)

    p = SpyPortfolio({"A": 200.0})
    mgr = MarketCrisisManager(portfolio=p)

    # Activate kill switch
    mgr.kill_switch(actor="auto")
    assert mgr.allow_trade() is False

    # Evaluate a trigger while killed — mitigation should be skipped
    metrics = {"volatility": 0.2, "correlation": 0.9, "liquidity": 0.1}
    res = mgr.evaluate(metrics, actor="system")
    assert res["triggered"] is False
    assert p.reduce_calls == 0

    # Now human override
    mgr.override_kill_switch(actor="alice")
    assert mgr.allow_trade() is True
    # Confirm override was logged
    assert any(
        entry
        for entry in mgr.audit_log
        if entry["action"] == "kill_switch_overridden" and entry["actor"] == "alice"
    )
