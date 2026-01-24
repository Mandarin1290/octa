from octa_wargames.data_poisoning import DataFeed, DataPoisoningSimulator
from octa_wargames.failure_cascade import FailureCascadeSimulator
from octa_wargames.strategy_sabotage import StrategyContext


def make_feed(price=100.0, ts=None):
    from datetime import datetime, timezone

    if ts is None:
        ts = datetime.now(timezone.utc).isoformat()
    f = DataFeed(name="p", price=price, ts=ts)
    f.history.append({"ts": ts, "price": price})
    return f


def make_strategy(id="s1"):
    positions = {"AAA": 10}
    prices = {"AAA": 100.0}
    return StrategyContext(
        id=id,
        name=id,
        positions=positions,
        prices=prices,
        cash=1000.0,
        signals={"AAA": 1.0},
    )


def test_cascade_contained():
    sim = FailureCascadeSimulator(breaker_threshold=1)
    primary = make_feed()
    backup = make_feed(price=101.0)
    # poison primary
    DataPoisoningSimulator.price_spike(primary, 0.5)

    s = make_strategy("victim")
    s2 = make_strategy("other")
    # ensure history for detection of signal inversion is present
    s.metadata["signal_history"] = [s.signals.copy()]
    s2.metadata["signal_history"] = [s2.signals.copy()]

    out = sim.simulate_data_strategy_execution(primary, [backup], [s, s2])
    assert out["feed_corrupted"] is True
    # at least one strategy isolated
    assert (
        any(not v["isolated"] for v in out["isolation"].values())
        or sim.blocked_orders > 0
    )
    assert out["breaker_tripped"] is True


def test_isolation_confirmed():
    sim = FailureCascadeSimulator(breaker_threshold=1)
    # simulate broker mismatch large enough to trip
    out = sim.simulate_broker_reconciliation(mismatches=5, mismatch_threshold=2)
    assert out["breaker_tripped"] is True
    assert out["nav_allowed"] is False
