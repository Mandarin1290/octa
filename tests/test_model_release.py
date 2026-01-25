from __future__ import annotations

from octa.core.governance.model_release import decide_release


def test_decide_release_pass() -> None:
    validation = {"ok": True, "aggregate_metrics": {"sharpe_cv": 0.2}}
    scoring = {"ok": True, "metrics": {"sharpe": 1.5, "max_drawdown": 0.05, "trade_count": 100}}
    mc = {"ok": True, "prob_dd_breach": 0.05}
    thresholds = {
        "min_sharpe": 1.0,
        "max_drawdown": 0.12,
        "min_trades": 50,
        "max_split_cv": 0.5,
        "mc_dd_prob_max": 0.1,
    }
    decision = decide_release(validation, scoring, mc, thresholds)
    assert decision.released
