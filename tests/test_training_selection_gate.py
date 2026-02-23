from __future__ import annotations

from octa.core.gates.training_selection_gate import (
    MetricRule,
    TrainingSelectionGateConfig,
    evaluate_training_selection,
)


def _cfg(**kwargs):
    base = TrainingSelectionGateConfig(
        enabled=True,
        min_population=30,
        top_percentile=0.70,
        fallback_top_k_if_empty=1,
        required_metrics=("sharpe", "profit_factor", "calmar", "max_drawdown"),
        metrics={
            "sharpe": MetricRule("high", 0.35),
            "profit_factor": MetricRule("high", 0.25),
            "calmar": MetricRule("high", 0.20),
            "max_drawdown": MetricRule("low", 0.20),
        },
    )
    data = base.__dict__.copy()
    data.update(kwargs)
    return TrainingSelectionGateConfig(**data)


def test_deterministic_tie_breaker_and_repeatable() -> None:
    candidates = [
        {"candidate_id": "B", "structural_pass": True, "static_pass": False, "metrics": {"sharpe": 1.0, "profit_factor": 1.1, "calmar": 0.5, "max_drawdown": 0.2}, "reasons": []},
        {"candidate_id": "A", "structural_pass": True, "static_pass": False, "metrics": {"sharpe": 1.0, "profit_factor": 1.1, "calmar": 0.5, "max_drawdown": 0.2}, "reasons": []},
        {"candidate_id": "C", "structural_pass": True, "static_pass": False, "metrics": {"sharpe": 1.0, "profit_factor": 1.1, "calmar": 0.5, "max_drawdown": 0.2}, "reasons": []},
    ]
    cfg = _cfg(top_percentile=1.01, fallback_top_k_if_empty=2)
    r1 = evaluate_training_selection(candidates=candidates, config=cfg)
    r2 = evaluate_training_selection(candidates=candidates, config=cfg)
    assert r1["decisions"] == r2["decisions"]
    picked = [d["candidate_id"] for d in r1["decisions"] if d["final_pass"]]
    assert picked == ["A", "B"]


def test_small_population_uses_fallback_when_dynamic_empty() -> None:
    candidates = [
        {"candidate_id": "AAA", "structural_pass": True, "static_pass": False, "metrics": {"sharpe": 0.9, "profit_factor": 1.01, "calmar": 0.4, "max_drawdown": 0.25}, "reasons": []},
        {"candidate_id": "BBB", "structural_pass": True, "static_pass": False, "metrics": {"sharpe": 0.8, "profit_factor": 1.00, "calmar": 0.3, "max_drawdown": 0.30}, "reasons": []},
        {"candidate_id": "CCC", "structural_pass": True, "static_pass": False, "metrics": {"sharpe": 0.7, "profit_factor": 0.99, "calmar": 0.2, "max_drawdown": 0.35}, "reasons": []},
    ]
    cfg = _cfg(top_percentile=1.01, fallback_top_k_if_empty=1)
    res = evaluate_training_selection(candidates=candidates, config=cfg)
    assert res["hard_fail"] is False
    assert res["fallback_used"] is True
    passed = [d["candidate_id"] for d in res["decisions"] if d["final_pass"]]
    assert passed == ["AAA"]


def test_hard_fail_when_missing_metrics_dominant() -> None:
    candidates = [
        {"candidate_id": "A", "structural_pass": True, "static_pass": False, "metrics": {"sharpe": None, "profit_factor": 1.0, "calmar": 0.4, "max_drawdown": 0.2}, "reasons": []},
        {"candidate_id": "B", "structural_pass": True, "static_pass": False, "metrics": {"sharpe": None, "profit_factor": None, "calmar": 0.4, "max_drawdown": 0.2}, "reasons": []},
        {"candidate_id": "C", "structural_pass": True, "static_pass": False, "metrics": {"sharpe": 0.7, "profit_factor": 1.0, "calmar": 0.4, "max_drawdown": 0.2}, "reasons": []},
    ]
    res = evaluate_training_selection(candidates=candidates, config=_cfg())
    assert res["hard_fail"] is True
    assert res["hard_fail_reason"] == "metrics_missing_or_nonfinite_dominant"


def test_disabled_mode_passthrough_static() -> None:
    cfg = _cfg(enabled=False)
    candidates = [
        {"candidate_id": "A", "structural_pass": True, "static_pass": True, "metrics": {}, "reasons": []},
        {"candidate_id": "B", "structural_pass": True, "static_pass": False, "metrics": {}, "reasons": []},
        {"candidate_id": "C", "structural_pass": False, "static_pass": True, "metrics": {}, "reasons": []},
    ]
    res = evaluate_training_selection(candidates=candidates, config=cfg)
    by_id = {d["candidate_id"]: d for d in res["decisions"]}
    assert by_id["A"]["final_pass"] is True
    assert by_id["B"]["final_pass"] is False
    assert by_id["C"]["final_pass"] is False


def test_n1_finite_metrics_passes_via_fallback() -> None:
    cfg = _cfg(top_percentile=1.01, min_population=30, fallback_top_k_if_empty=1)
    candidates = [
        {
            "candidate_id": "ONE",
            "structural_pass": True,
            "static_pass": False,
            "metrics": {"sharpe": 0.8, "pf": 1.2, "calmar": 0.4, "max_dd": 0.2},
            "reasons": [],
        }
    ]
    out = evaluate_training_selection(candidates=candidates, config=cfg)
    assert out["hard_fail"] is False
    assert out["fallback_used"] is True
    d = out["decisions"][0]
    assert d["final_pass"] is True
    assert d["fallback_selected"] is True


def test_n1_nan_required_metric_fails_with_missing_required_metric() -> None:
    cfg = _cfg(min_population=30, fallback_top_k_if_empty=1)
    candidates = [
        {
            "candidate_id": "ONE",
            "structural_pass": True,
            "static_pass": False,
            "metrics": {"sharpe": float("nan"), "profit_factor": 1.2, "calmar": 0.4, "max_drawdown": 0.2},
            "reasons": [],
        }
    ]
    out = evaluate_training_selection(candidates=candidates, config=cfg)
    assert out["hard_fail"] is True
    d = out["decisions"][0]
    assert d["final_pass"] is False
    assert d["reject_reason_code"] == "missing_required_metric"
