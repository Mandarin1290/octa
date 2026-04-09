from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd

from octa_training.core.evaluation import EvalSettings
from octa_training.core.institutional_gates import (
    evaluate_cost_stress,
    evaluate_cross_timeframe_consistency,
    evaluate_liquidity_gate,
    evaluate_regime_stability,
    evaluate_walk_forward_oos,
)


def _gate(**overrides):
    base = {
        "profit_factor_min": 1.1,
        "sharpe_min": 0.4,
        "max_drawdown_max": 0.2,
        "walkforward_oos_pf_scale": 0.95,
        "walkforward_oos_sharpe_scale": 0.9,
        "walkforward_oos_dd_scale": 1.0,
        "walkforward_min_fold_pass_ratio": 1.0,
        "regime_pf_min": 1.1,
        "regime_pf_min_worst": 1.0,
        "regime_sharpe_collapse_ratio": 0.35,
        "stress_pf_min": 1.05,
        "stress_dd_mult": 1.25,
        "liquidity_percentile_min": 40.0,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _mk_backtest(n: int, *, mu: float, sigma: float, seed: int = 7) -> pd.DataFrame:
    idx = pd.date_range("2020-01-01", periods=n, freq="D", tz="UTC")
    rng = np.random.default_rng(seed)
    strat = rng.normal(mu, sigma, size=n)
    ret = rng.normal(0.0005, 0.01, size=n)
    price = 100.0 * np.exp(np.cumsum(ret))
    turnover = np.abs(rng.normal(0.2, 0.05, size=n))
    return pd.DataFrame({"price": price, "ret": ret, "strat_ret": strat, "turnover": turnover}, index=idx)


def test_walk_forward_missing_is_fail_closed() -> None:
    idx = pd.date_range("2020-01-01", periods=300, freq="D", tz="UTC")
    df = pd.DataFrame({"ret": np.zeros(len(idx))}, index=idx)
    out = evaluate_walk_forward_oos(df, _gate(), timeframe="1D")
    assert out["passed"] is False
    assert out["reason"] == "walkforward_missing_strat_ret"


def test_walk_forward_insufficient_history_fails() -> None:
    df = _mk_backtest(200, mu=0.001, sigma=0.01)
    out = evaluate_walk_forward_oos(df, _gate(), timeframe="1D")
    assert out["passed"] is False
    assert out["reason"] == "insufficient_history_for_walkforward"


def test_walk_forward_oos_poor_metrics_fail() -> None:
    df = _mk_backtest(600, mu=-0.001, sigma=0.01, seed=11)
    out = evaluate_walk_forward_oos(df, _gate(profit_factor_min=1.3, sharpe_min=1.0), timeframe="1D")
    assert out["passed"] is False
    assert out["reason"] == "walkforward_oos_threshold_failed"


def test_regime_stability_detects_collapse() -> None:
    n = 700
    idx = pd.date_range("2020-01-01", periods=n, freq="D", tz="UTC")
    rng = np.random.default_rng(123)
    ret = rng.normal(0.0004, 0.005, size=n)
    ret[-220:] = rng.normal(0.0001, 0.03, size=220)
    strat = rng.normal(0.0012, 0.003, size=n)
    strat[-220:] = rng.normal(-0.004, 0.02, size=220)
    price = 100.0 * np.exp(np.cumsum(ret))
    df = pd.DataFrame({"price": price, "ret": ret, "strat_ret": strat, "turnover": np.ones(n)}, index=idx)
    wf = evaluate_walk_forward_oos(df, _gate(), timeframe="1D")
    out = evaluate_regime_stability(df, _gate(), timeframe="1D", walkforward_meta=wf.get("walkforward_meta"))
    assert out["passed"] is False
    assert out["reason"] in {"regime_high_failed", "regime_mid_failed", "regime_low_failed", "regime_sharpe_collapse"}


def test_cost_stress_fails_when_pf_breaks() -> None:
    n = 700
    idx = pd.date_range("2020-01-01", periods=n, freq="D", tz="UTC")
    rng = np.random.default_rng(42)
    ret = rng.normal(0.00025, 0.01, size=n)
    preds = pd.Series(rng.normal(0.0, 0.2, size=n), index=idx)
    price = pd.Series(100.0 * np.exp(np.cumsum(ret)), index=idx)
    settings = EvalSettings(mode="cls", upper_q=0.55, lower_q=0.45, cost_bps=20.0, spread_bps=15.0)
    out = evaluate_cost_stress(price, preds, settings, _gate(stress_pf_min=1.2))
    assert out["passed"] is False
    assert out["reason"] in {"stress_pf_below_min", "stress_monthly_net_non_positive", "stress_dd_above_limit"}


def test_liquidity_equity_low_volume_fails() -> None:
    idx = pd.date_range("2020-01-01", periods=400, freq="D", tz="UTC")
    vol = np.linspace(1000.0, 1.0, len(idx))
    df = pd.DataFrame({"close": np.linspace(100, 101, len(idx)), "volume": vol}, index=idx)
    out = evaluate_liquidity_gate(df, timeframe="1D", gate=_gate(liquidity_percentile_min=80.0), asset_class="stock")
    assert out["passed"] is False
    assert out["reason"] == "liquidity_percentile_below_threshold"


def test_liquidity_fx_no_volume_passes_unknown() -> None:
    idx = pd.date_range("2020-01-01", periods=400, freq="h", tz="UTC")
    df = pd.DataFrame({"close": np.linspace(1.1, 1.2, len(idx))}, index=idx)
    out = evaluate_liquidity_gate(df, timeframe="1H", gate=_gate(), asset_class="fx")
    assert out["passed"] is True
    assert out["liquidity_unknown"] is True
    assert out["liquidity_method"] == "na"


def _stage(tf: str, *, cagr: float, dd: float, gate_pass: bool = True) -> dict:
    chk = {"passed": gate_pass}
    return {
        "timeframe": tf,
        "status": "PASS" if gate_pass else "FAIL",
        "metrics_summary": {"cagr": cagr, "max_drawdown": dd},
        "monte_carlo": chk,
        "walk_forward": chk,
        "regime_stability": chk,
        "cost_stress": chk,
        "liquidity": chk,
    }


def test_cross_tf_consistency_fails_on_contradiction() -> None:
    stages = [
        _stage("1D", cagr=0.12, dd=0.05),
        _stage("1H", cagr=-0.08, dd=0.04),
        _stage("30M", cagr=0.02, dd=0.03),
        _stage("5M", cagr=0.01, dd=0.03),
        _stage("1M", cagr=0.01, dd=0.02),
    ]
    out = evaluate_cross_timeframe_consistency(stages)
    assert out["executed"] is True
    assert out["passed"] is False
    assert out["reason"] == "cross_tf_inconsistent"


# ---------------------------------------------------------------------------
# regime_stability_skip_low
# ---------------------------------------------------------------------------


def _mk_reit_backtest(n: int = 700, seed: int = 42) -> pd.DataFrame:
    """Build a backtest where low-vol regime is bad (pf<1) but mid/high are strong.

    Simulates a REIT-like strategy: good in volatile markets, weak in calm ones.
    """
    idx = pd.date_range("2020-01-01", periods=n, freq="D", tz="UTC")
    rng = np.random.default_rng(seed)
    ret = np.empty(n)
    strat = np.empty(n)
    # Interleave low-vol and high-vol blocks so all three regimes are present
    block = n // 3
    # low-vol block: small returns, bad strategy
    ret[:block] = rng.normal(0.0001, 0.003, size=block)
    strat[:block] = rng.normal(-0.001, 0.003, size=block)
    # mid-vol block: moderate returns, good strategy
    ret[block : 2 * block] = rng.normal(0.0005, 0.010, size=block)
    strat[block : 2 * block] = rng.normal(0.003, 0.006, size=block)
    # high-vol block: large returns, strong strategy
    ret[2 * block :] = rng.normal(0.0008, 0.020, size=n - 2 * block)
    strat[2 * block :] = rng.normal(0.006, 0.012, size=n - 2 * block)
    price = 100.0 * np.exp(np.cumsum(ret))
    return pd.DataFrame(
        {"price": price, "ret": ret, "strat_ret": strat, "turnover": np.ones(n)},
        index=idx,
    )


def test_regime_skip_low_false_fails_low_vol() -> None:
    """Without skip_low, REIT-like backtest fails regime gate due to low-vol segment."""
    df = _mk_reit_backtest()
    gate = _gate(regime_pf_min=1.1, regime_sharpe_collapse_ratio=0.10)
    out = evaluate_regime_stability(df, gate, timeframe="1D")
    assert out["passed"] is False
    assert any("low" in r or "collapse" in r for r in out.get("reasons", []))


def test_regime_skip_low_true_passes_when_mid_high_strong() -> None:
    """With skip_low=True, REIT-like backtest passes if mid/high regimes are healthy."""
    df = _mk_reit_backtest()
    gate = _gate(
        regime_pf_min=1.1,
        regime_sharpe_collapse_ratio=0.10,
        regime_stability_skip_low=True,
    )
    out = evaluate_regime_stability(df, gate, timeframe="1D")
    assert out["passed"] is True, f"Expected pass, reasons={out.get('reasons')}"


def test_regime_skip_low_still_fails_on_mid_failure() -> None:
    """skip_low=True must NOT suppress mid/high failures."""
    df = _mk_reit_backtest()
    # Require extremely high PF — even mid/high can't meet this
    gate = _gate(
        regime_pf_min=10.0,
        regime_pf_min_worst=10.0,
        regime_sharpe_collapse_ratio=0.10,
        regime_stability_skip_low=True,
    )
    out = evaluate_regime_stability(df, gate, timeframe="1D")
    assert out["passed"] is False
    assert any("mid" in r or "high" in r for r in out.get("reasons", []))


def test_regime_skip_low_meta_flag_present() -> None:
    """regime_meta must expose the skip_low flag for audit traceability."""
    df = _mk_reit_backtest()
    gate = _gate(regime_stability_skip_low=True)
    out = evaluate_regime_stability(df, gate, timeframe="1D")
    assert out["regime_meta"].get("regime_stability_skip_low") is True


def test_regime_skip_low_default_false_unchanged_behaviour() -> None:
    """GateSpec without regime_stability_skip_low behaves exactly as before."""
    df = _mk_reit_backtest()
    gate_old = _gate()  # no regime_stability_skip_low key → defaults to False
    gate_new = _gate(regime_stability_skip_low=False)
    out_old = evaluate_regime_stability(df, gate_old, timeframe="1D")
    out_new = evaluate_regime_stability(df, gate_new, timeframe="1D")
    assert out_old["passed"] == out_new["passed"]
    assert out_old.get("reasons") == out_new.get("reasons")


# ---------------------------------------------------------------------------
# regime_stability_skip_high
# ---------------------------------------------------------------------------


def _mk_intraday_backtest(n: int = 1200, seed: int = 99) -> pd.DataFrame:
    """Build a backtest where high-vol regime is bad (pf<1) but low/mid are strong.

    Simulates a 1H intraday strategy: profitable in calm markets, breaks in VIX spikes.
    Uses 1200 bars to exceed 1H min_bars_regime=960.
    """
    idx = pd.date_range("2020-01-01", periods=n, freq="h", tz="UTC")
    rng = np.random.default_rng(seed)
    ret = np.empty(n)
    strat = np.empty(n)
    block = n // 3
    # low-vol block: calm, strong strategy
    ret[:block] = rng.normal(0.0002, 0.004, size=block)
    strat[:block] = rng.normal(0.005, 0.005, size=block)
    # mid-vol block: moderate vol, good strategy
    ret[block : 2 * block] = rng.normal(0.0003, 0.008, size=block)
    strat[block : 2 * block] = rng.normal(0.003, 0.006, size=block)
    # high-vol block: VIX-spike, strategy loses (wide spreads, noise)
    ret[2 * block :] = rng.normal(0.0005, 0.020, size=n - 2 * block)
    strat[2 * block :] = rng.normal(-0.002, 0.015, size=n - 2 * block)
    price = 100.0 * np.exp(np.cumsum(ret))
    return pd.DataFrame(
        {"price": price, "ret": ret, "strat_ret": strat, "turnover": np.ones(n)},
        index=idx,
    )


def test_regime_skip_high_false_fails_high_vol() -> None:
    """Without skip_high, intraday backtest fails regime gate on high-vol segment."""
    df = _mk_intraday_backtest()
    gate = _gate(regime_pf_min=1.0, regime_pf_min_worst=1.0, regime_sharpe_collapse_ratio=0.10)
    out = evaluate_regime_stability(df, gate, timeframe="1H")
    assert out["passed"] is False
    assert any("high" in r or "collapse" in r for r in out.get("reasons", []))


def test_regime_skip_high_true_passes_when_low_mid_strong() -> None:
    """With skip_high=True, intraday backtest passes if low/mid regimes are healthy."""
    df = _mk_intraday_backtest()
    gate = _gate(
        regime_pf_min=1.0,
        regime_pf_min_worst=1.0,
        regime_sharpe_collapse_ratio=0.10,
        regime_stability_skip_high=True,
    )
    out = evaluate_regime_stability(df, gate, timeframe="1H")
    assert out["passed"] is True, f"Expected pass, reasons={out.get('reasons')}"


def test_regime_skip_high_still_fails_on_mid_failure() -> None:
    """skip_high=True must NOT suppress low/mid failures."""
    df = _mk_intraday_backtest()
    gate = _gate(
        regime_pf_min=10.0,  # impossible threshold — mid will fail
        regime_pf_min_worst=10.0,
        regime_sharpe_collapse_ratio=0.10,
        regime_stability_skip_high=True,
    )
    out = evaluate_regime_stability(df, gate, timeframe="1H")
    assert out["passed"] is False
    assert any("low" in r or "mid" in r for r in out.get("reasons", []))


def test_regime_skip_high_meta_flag_present() -> None:
    """regime_meta must expose the skip_high flag for audit traceability."""
    df = _mk_intraday_backtest()
    gate = _gate(regime_stability_skip_high=True)
    out = evaluate_regime_stability(df, gate, timeframe="1H")
    assert out["regime_meta"].get("regime_stability_skip_high") is True


def test_regime_skip_high_default_false_unchanged_behaviour() -> None:
    """GateSpec without regime_stability_skip_high behaves exactly as before."""
    df = _mk_intraday_backtest()
    gate_old = _gate()
    gate_new = _gate(regime_stability_skip_high=False)
    out_old = evaluate_regime_stability(df, gate_old, timeframe="1H")
    out_new = evaluate_regime_stability(df, gate_new, timeframe="1H")
    assert out_old["passed"] == out_new["passed"]
    assert out_old.get("reasons") == out_new.get("reasons")


def test_cross_tf_consistency_passes_when_aligned() -> None:
    stages = [
        _stage("1D", cagr=0.12, dd=0.05),
        _stage("1H", cagr=0.06, dd=0.04),
        _stage("30M", cagr=0.02, dd=0.03),
        _stage("5M", cagr=0.01, dd=0.03),
        _stage("1M", cagr=0.01, dd=0.02),
    ]
    out = evaluate_cross_timeframe_consistency(stages)
    assert out["executed"] is True
    assert out["passed"] is True
