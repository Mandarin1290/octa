from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd

from octa_training.core.robustness import mandatory_monte_carlo_gate


def _gate(**overrides):
    base = {
        "monte_carlo_n": 500,
        "monte_carlo_seed": 1337,
        "monte_carlo_pf_p05_min": 1.05,
        "monte_carlo_sharpe_p05_min": 0.40,
        "monte_carlo_maxdd_mult": 1.5,
        "monte_carlo_prob_loss_max": 0.40,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_monte_carlo_missing_strat_ret_fails_closed() -> None:
    idx = pd.date_range("2020-01-01", periods=120, freq="D", tz="UTC")
    df = pd.DataFrame({"ret": np.zeros(len(idx))}, index=idx)
    out = mandatory_monte_carlo_gate(df, metrics=SimpleNamespace(max_drawdown=0.1), gate=_gate())
    assert out["passed"] is False
    assert out["reason"] == "monte_carlo_missing_strat_ret"


def test_monte_carlo_threshold_breach_fails() -> None:
    idx = pd.date_range("2020-01-01", periods=200, freq="D", tz="UTC")
    rng = np.random.default_rng(123)
    bad = rng.normal(loc=-0.001, scale=0.01, size=len(idx))
    df = pd.DataFrame({"strat_ret": bad, "turnover": np.ones(len(idx))}, index=idx)
    out = mandatory_monte_carlo_gate(
        df,
        metrics=SimpleNamespace(max_drawdown=0.05),
        gate=_gate(monte_carlo_pf_p05_min=1.5, monte_carlo_sharpe_p05_min=1.0, monte_carlo_prob_loss_max=0.1),
    )
    assert out["passed"] is False
    assert out["reason"] == "monte_carlo_gate_failed"
    assert out["reasons"]


def test_monte_carlo_moderate_metrics_pass() -> None:
    idx = pd.date_range("2020-01-01", periods=300, freq="D", tz="UTC")
    rng = np.random.default_rng(7)
    good = rng.normal(loc=0.004, scale=0.003, size=len(idx))
    df = pd.DataFrame({"strat_ret": good, "turnover": np.ones(len(idx))}, index=idx)
    out = mandatory_monte_carlo_gate(df, metrics=SimpleNamespace(max_drawdown=0.08), gate=_gate())
    assert out["enabled"] is True
    assert out["passed"] is True
    assert out["metrics"]["mc_pf_p05"] >= 1.05
