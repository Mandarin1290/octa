import pytest

from octa_security.config_drift import ConfigBaseline, ConfigDriftException


def test_drift_detected_param_violation():
    baseline = {"max_trade_size": 1000, "symbols": ["EURUSD"]}
    cb = ConfigBaseline(baseline, allowed_ranges={"max_trade_size": (0, 10000)})

    # Within range -> no exception
    current_ok = {"max_trade_size": 5000, "symbols": ["EURUSD"]}
    assert cb.enforce(current_ok) is True

    # Above max -> should raise because of param violation
    current_bad = {"max_trade_size": 20000, "symbols": ["EURUSD"]}
    with pytest.raises(ConfigDriftException) as excinfo:
        cb.enforce(current_bad)
    assert "Parameter boundary violation" in str(excinfo.value)


def test_enforcement_requires_approvals_for_hash_mismatch():
    baseline = {"max_trade_size": 1000, "symbols": ["EURUSD"]}
    cb = ConfigBaseline(baseline, required_approvals={"ops"})

    # Modify a parameter that is not in allowed_ranges -> hash mismatch
    current = {"max_trade_size": 1000, "symbols": ["EURUSD", "GBPUSD"]}

    # Without approvals -> blocked
    with pytest.raises(ConfigDriftException) as excinfo:
        cb.enforce(current, approvals=None)
    assert "required approvals missing" in str(excinfo.value).lower()

    # With required approval -> allowed
    assert cb.enforce(current, approvals=["ops"]) is True
