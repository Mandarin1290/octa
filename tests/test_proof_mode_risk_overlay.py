from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from octa_training.core.robustness import run_risk_overlay_tests


def _df(rows: int) -> pd.DataFrame:
    idx = pd.date_range("2020-01-01", periods=rows, freq="D", tz="UTC")
    return pd.DataFrame(
        {
            "price": [100.0] * rows,
            "ret": [0.001] * rows,
            "strat_ret": [0.001] * rows,
            "volume": [1000.0] * rows,
        },
        index=idx,
    )


def test_proof_mode_downgrades_insufficient_oos_history_only(monkeypatch) -> None:
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_walk_forward_oos",
        lambda *args, **kwargs: {
            "enabled": True,
            "passed": False,
            "reason": "insufficient_history_for_walkforward",
            "walkforward_meta": {"required_bars_for_2": 378},
        },
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_regime_stability",
        lambda *args, **kwargs: {
            "enabled": True,
            "passed": False,
            "reason": "regime_split_insufficient_bars",
            "regime_meta": {"min_bars": 150},
        },
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_cost_stress",
        lambda *args, **kwargs: {"enabled": True, "passed": True, "reason": None},
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_liquidity_gate",
        lambda *args, **kwargs: {"enabled": True, "passed": True, "reason": None},
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.mandatory_monte_carlo_gate",
        lambda *args, **kwargs: {"enabled": True, "passed": True, "reason": None, "reasons": []},
    )

    res = run_risk_overlay_tests(
        _df(80),
        _df(80)["strat_ret"],
        SimpleNamespace(n_trades=12),
        SimpleNamespace(),
        SimpleNamespace(proof_mode=True),
        source_df=_df(500),
        asset_class="equities",
        timeframe="1D",
        proof_mode=True,
    )
    assert res.passed is True
    assert "proof_mode:walkforward_insufficient_oos_history" in res.limited_reasons
    assert "proof_mode:regime_insufficient_oos_history" in res.limited_reasons


def test_non_proof_mode_keeps_insufficient_oos_history_blocking(monkeypatch) -> None:
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_walk_forward_oos",
        lambda *args, **kwargs: {
            "enabled": True,
            "passed": False,
            "reason": "insufficient_history_for_walkforward",
            "walkforward_meta": {"required_bars_for_2": 378},
        },
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_regime_stability",
        lambda *args, **kwargs: {
            "enabled": True,
            "passed": False,
            "reason": "regime_split_insufficient_bars",
            "regime_meta": {"min_bars": 150},
        },
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_cost_stress",
        lambda *args, **kwargs: {"enabled": True, "passed": True, "reason": None},
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_liquidity_gate",
        lambda *args, **kwargs: {"enabled": True, "passed": True, "reason": None},
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.mandatory_monte_carlo_gate",
        lambda *args, **kwargs: {"enabled": True, "passed": True, "reason": None, "reasons": []},
    )

    res = run_risk_overlay_tests(
        _df(80),
        _df(80)["strat_ret"],
        SimpleNamespace(n_trades=12),
        SimpleNamespace(),
        SimpleNamespace(proof_mode=False),
        source_df=_df(500),
        asset_class="equities",
        timeframe="1D",
        proof_mode=False,
    )
    assert res.passed is False
    assert "insufficient_history_for_walkforward" in res.reasons


def test_proof_mode_does_not_bypass_tail_or_other_real_failures(monkeypatch) -> None:
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_walk_forward_oos",
        lambda *args, **kwargs: {
            "enabled": True,
            "passed": False,
            "reason": "insufficient_history_for_walkforward",
            "walkforward_meta": {"required_bars_for_2": 378},
        },
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_regime_stability",
        lambda *args, **kwargs: {
            "enabled": True,
            "passed": False,
            "reason": "regime_split_insufficient_bars",
            "regime_meta": {"min_bars": 150},
        },
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_cost_stress",
        lambda *args, **kwargs: {"enabled": True, "passed": False, "reason": "cost_stress_failed"},
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.evaluate_liquidity_gate",
        lambda *args, **kwargs: {"enabled": True, "passed": True, "reason": None},
    )
    monkeypatch.setattr(
        "octa_training.core.robustness.mandatory_monte_carlo_gate",
        lambda *args, **kwargs: {"enabled": True, "passed": True, "reason": None, "reasons": []},
    )

    res = run_risk_overlay_tests(
        _df(80),
        _df(80)["strat_ret"],
        SimpleNamespace(n_trades=12),
        SimpleNamespace(),
        SimpleNamespace(proof_mode=True),
        source_df=_df(500),
        asset_class="equities",
        timeframe="1D",
        proof_mode=True,
    )
    assert res.passed is False
    assert "cost_stress_failed" in res.reasons
