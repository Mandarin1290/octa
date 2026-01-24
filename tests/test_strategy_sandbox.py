import time

from octa_core.sandbox import StrategySandbox
from octa_core.strategy import StrategyInput, StrategySpec


def test_baseline_strategy_valid(tmp_path):
    spec = StrategySpec(id="s1", universe=["AAA"], frequency="1m", risk_budget=0.5)
    prices = {"AAA": [100, 101, 102, 103, 104, 105]}
    inp = StrategyInput(
        timestamp="2025-01-01T00:00:00Z", features={}, prices=prices, regime={}
    )
    from octa_strategies.baseline_trend import run_strategy

    sb = StrategySandbox(timeout_sec=2.0, memory_mb=50)
    out = sb.run(run_strategy, spec, inp)
    assert "exposures" in out.to_dict()
    assert 0.0 <= out.confidence <= 1.0


def test_invalid_output_rejected(tmp_path):
    spec = StrategySpec(id="s2", universe=["AAA"], frequency="1m", risk_budget=0.5)
    prices = {"AAA": [100, 101, 102, 103, 104, 105]}
    inp = StrategyInput(
        timestamp="2025-01-01T00:00:00Z", features={}, prices=prices, regime={}
    )

    def bad_strategy(spec, inp, state):
        # returns exposure outside [-1,1]
        return {"exposures": {"AAA": 2.0}, "confidence": 0.5}

    sb = StrategySandbox(timeout_sec=2.0, memory_mb=50)
    try:
        sb.run(bad_strategy, spec, inp)
        raise AssertionError("Expected ValueError for invalid exposures")
    except ValueError:
        pass


def test_timeout_enforced(tmp_path):
    spec = StrategySpec(id="s3", universe=["AAA"], frequency="1m", risk_budget=0.5)
    prices = {"AAA": [100, 101, 102, 103, 104, 105]}
    inp = StrategyInput(
        timestamp="2025-01-01T00:00:00Z", features={}, prices=prices, regime={}
    )

    def slow_strategy(spec, inp, state):
        time.sleep(3)
        return {"exposures": {"AAA": 0.1}, "confidence": 0.5}

    sb = StrategySandbox(timeout_sec=1.0, memory_mb=50)
    try:
        sb.run(slow_strategy, spec, inp)
        raise AssertionError("Expected TimeoutError")
    except TimeoutError:
        pass
