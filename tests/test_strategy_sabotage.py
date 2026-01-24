from octa_wargames.strategy_sabotage import (
    SabotageSimulator,
    StrategyContext,
    StrategyMonitor,
)


def make_strategy(name="s1"):
    positions = {"AAA": 10, "BBB": 5}
    prices = {"AAA": 10.0, "BBB": 20.0}
    cash = 1000.0
    signals = {"AAA": 0.5, "BBB": -0.2}
    # include history matching current signals (positive agreement)
    metadata = {"signal_history": [signals.copy(), signals.copy(), signals.copy()]}
    return StrategyContext(
        id=name,
        name=name,
        positions=positions,
        prices=prices,
        cash=cash,
        signals=signals,
        metadata=metadata,
    )


def test_sabotage_detected_and_isolated_inverted_signals():
    s = make_strategy("victim")
    other = make_strategy("other")

    SabotageSimulator.invert_signals(s)

    monitor = StrategyMonitor(leverage_threshold=10.0)
    results = monitor.assess_and_isolate([s, other])

    assert results["victim"]["inverted"] is True
    assert results["victim"]["isolated"] is True
    assert s.active is False
    # other strategy should remain active (no contagion)
    assert results["other"]["isolated"] is False
    assert other.active is True


def test_runaway_leverage_triggers_isolation():
    s = make_strategy("runaway")
    other = make_strategy("safe")

    # apply large multiplier to ensure leverage > threshold
    SabotageSimulator.runaway_leverage(s, multiplier=50.0)

    monitor = StrategyMonitor(leverage_threshold=2.0)
    results = monitor.assess_and_isolate([s, other])

    assert results["runaway"]["runaway"] is True
    assert results["runaway"]["isolated"] is True
    assert s.active is False
    assert other.active is True


def test_stuck_positions_detected_and_isolated():
    s = make_strategy("stuck")
    other = make_strategy("healthy")

    # mark stuck and remove recent trades
    SabotageSimulator.stuck_positions(s)
    s.recent_trades = []

    monitor = StrategyMonitor(inactivity_ticks=1)
    results = monitor.assess_and_isolate([s, other])

    assert results["stuck"]["stuck"] is True
    assert results["stuck"]["isolated"] is True
    assert s.active is False
    assert other.active is True
