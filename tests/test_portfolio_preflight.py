"""Tests for portfolio pre-flight overlay."""

from __future__ import annotations

import random

import pytest

from octa.core.portfolio.preflight import (
    PreflightConfig,
    PreflightResult,
    _empirical_cvar,
    run_preflight,
)


def _deterministic_returns(seed: int, n: int = 100) -> list[float]:
    rng = random.Random(seed)
    return [rng.gauss(0.0005, 0.02) for _ in range(n)]


def test_preflight_all_ok() -> None:
    returns = {
        "AAPL": _deterministic_returns(42),
        "MSFT": _deterministic_returns(43),
    }
    result = run_preflight(
        positions={"AAPL": 5000.0, "MSFT": 5000.0},
        nav=100000.0,
        returns_by_symbol=returns,
    )
    assert result.ok is True
    assert result.reason == "PREFLIGHT_OK"
    assert result.blocked_symbols == []


def test_preflight_symbol_exposure_exceeded() -> None:
    result = run_preflight(
        positions={"AAPL": 15000.0},
        nav=100000.0,
        returns_by_symbol={"AAPL": _deterministic_returns(42)},
        config=PreflightConfig(max_symbol_exposure_pct=0.10),
    )
    assert result.ok is False
    assert "SYMBOL_EXPOSURE_EXCEEDED" in result.reason
    assert "AAPL" in result.blocked_symbols


def test_preflight_gross_exposure_exceeded() -> None:
    positions = {f"SYM{i}": 20000.0 for i in range(10)}
    returns = {f"SYM{i}": _deterministic_returns(i) for i in range(10)}
    result = run_preflight(
        positions=positions,
        nav=100000.0,
        returns_by_symbol=returns,
        config=PreflightConfig(
            max_gross_exposure_pct=1.50,
            max_symbol_exposure_pct=0.25,
        ),
    )
    assert result.ok is False
    assert "GROSS_EXPOSURE_EXCEEDED" in result.reason


def test_preflight_net_exposure_exceeded() -> None:
    result = run_preflight(
        positions={"AAPL": 60000.0, "MSFT": 50000.0},
        nav=100000.0,
        returns_by_symbol={
            "AAPL": _deterministic_returns(42),
            "MSFT": _deterministic_returns(43),
        },
        config=PreflightConfig(
            max_net_exposure_pct=1.00,
            max_symbol_exposure_pct=0.70,
            max_gross_exposure_pct=2.0,
        ),
    )
    assert result.ok is False
    assert "NET_EXPOSURE_EXCEEDED" in result.reason


def test_preflight_correlation_exceeded() -> None:
    # Use identical returns to force high correlation
    returns_a = _deterministic_returns(42, 100)
    returns_b = [r + 0.0001 for r in returns_a]  # near-identical
    result = run_preflight(
        positions={"A": 5000.0, "B": 5000.0},
        nav=100000.0,
        returns_by_symbol={"A": returns_a, "B": returns_b},
        config=PreflightConfig(max_pairwise_correlation=0.80),
    )
    assert result.ok is False
    assert "CORRELATION_EXCEEDED" in result.reason


def test_preflight_unknown_correlation_fail_closed() -> None:
    # Two symbols but only one has returns
    result = run_preflight(
        positions={"AAPL": 5000.0, "MYSTERY": 5000.0},
        nav=100000.0,
        returns_by_symbol={"AAPL": _deterministic_returns(42)},
    )
    assert result.ok is False
    assert "UNKNOWN_CORRELATION" in result.reason


def test_preflight_unknown_tail_risk_fail_closed() -> None:
    # Multiple positions but no returns at all
    result = run_preflight(
        positions={"A": 5000.0, "B": 5000.0},
        nav=100000.0,
        returns_by_symbol={},
    )
    assert result.ok is False
    assert "UNKNOWN_TAIL_RISK" in result.reason


def test_preflight_nav_invalid() -> None:
    result = run_preflight(
        positions={"AAPL": 5000.0},
        nav=0.0,
        returns_by_symbol={},
    )
    assert result.ok is False
    assert result.reason == "NAV_INVALID"


def test_preflight_tail_risk_exceeded() -> None:
    # Create very volatile returns to trigger CVaR gate
    rng = random.Random(99)
    bad_returns = [rng.gauss(-0.05, 0.10) for _ in range(100)]
    result = run_preflight(
        positions={"BAD": 5000.0, "WORSE": 5000.0},
        nav=100000.0,
        returns_by_symbol={"BAD": bad_returns, "WORSE": bad_returns},
        config=PreflightConfig(max_cvar_pct=0.001),
    )
    assert result.ok is False
    assert "TAIL_RISK_EXCEEDED" in result.reason or "CORRELATION_EXCEEDED" in result.reason


def test_empirical_cvar_basic() -> None:
    # Simple test: uniform bad returns
    returns = [-0.10, -0.08, -0.06, -0.04, -0.02, 0.00, 0.02, 0.04, 0.06, 0.08]
    cvar = _empirical_cvar(returns, 0.95)
    assert cvar is not None
    assert cvar > 0  # CVaR should be positive (loss magnitude)


def test_empirical_cvar_all_positive() -> None:
    returns = [0.01, 0.02, 0.03, 0.04, 0.05]
    cvar = _empirical_cvar(returns, 0.95)
    assert cvar is not None
    assert cvar == 0.0  # No tail loss


def test_empirical_cvar_empty() -> None:
    assert _empirical_cvar([], 0.95) is None
    assert _empirical_cvar([0.01], 0.95) is None


def test_preflight_single_position_no_tail_block() -> None:
    # Single position with no returns should not block on UNKNOWN_TAIL_RISK
    # (fail-closed only when multiple positions and positive exposure)
    result = run_preflight(
        positions={"AAPL": 5000.0},
        nav=100000.0,
        returns_by_symbol={},
    )
    # Single position: no correlation block, no tail block
    assert result.ok is True


def test_preflight_deterministic() -> None:
    """Same inputs produce same outputs."""
    kwargs = dict(
        positions={"AAPL": 5000.0, "MSFT": 5000.0},
        nav=100000.0,
        returns_by_symbol={
            "AAPL": _deterministic_returns(42),
            "MSFT": _deterministic_returns(43),
        },
    )
    r1 = run_preflight(**kwargs)
    r2 = run_preflight(**kwargs)
    assert r1.ok == r2.ok
    assert r1.reason == r2.reason
    assert r1.checks == r2.checks
