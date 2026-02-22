from __future__ import annotations

import csv
import json

from scripts.octa_autopilot import (
    _append_stage_progress,
    _apply_stage_salvage,
    _check_stage_invariants,
    _debug_symbol_scope,
    _deterministic_train_order,
    _emit_symbol_step,
    _first20_symbols,
    _select_training_subset,
    _load_training_budget_cfg,
    _normalize_dynamic_hard_fail_reason,
    _is_non_overlapping_window,
    _resolve_stage_pool,
    _resolve_max_train_cap_for_tf,
    _resolve_runtime_timeframes,
    _top_fail_reasons,
    _should_continue_on_training_timeout,
    _fail_stage_empty,
    _stage_state_init,
    _write_oos_degradation_autopsy,
    _write_stage_state,
    _write_tf_precheck_audit,
    _write_stage_filter_state,
    _write_structural_audit_artifacts,
    _write_training_selection_artifacts,
)
from octa_ops.autopilot.types import GateDecision


def test_structural_audit_artifacts_written_even_when_zero_pass(tmp_path) -> None:
    run_dir = tmp_path / "run"
    rows = [
        {
            "timeframe": "1D",
            "symbol_id": "BBB",
            "structural_pass": False,
            "failing_rule_ids": ["pre_gate_data_quality:spacing_not_timeframe_like"],
            "failing_rule_values": {"data_quality": {"match_frac": 0.7}},
            "notes": "excluded_before_structural_training",
        }
    ]
    _write_structural_audit_artifacts(
        run_dir,
        rows,
        {"1D": {"pre_gate_data_quality:spacing_not_timeframe_like": 1}},
    )
    assert (run_dir / "structural_decisions.csv").exists()
    assert (run_dir / "structural_reason_counts.json").exists()
    assert (run_dir / "structural_autopsy_first20.json").exists()
    assert (run_dir / "structural_gate_summary.md").exists()
    rc = json.loads((run_dir / "structural_reason_counts.json").read_text(encoding="utf-8"))
    assert rc["overall"]["pre_gate_data_quality:spacing_not_timeframe_like"] == 1


def test_structural_audit_output_is_deterministic_sorted(tmp_path) -> None:
    run_dir = tmp_path / "run2"
    rows = [
        {
            "timeframe": "1D",
            "symbol_id": "ZZZ",
            "structural_pass": False,
            "failing_rule_ids": ["a"],
            "failing_rule_values": {},
            "notes": "",
        },
        {
            "timeframe": "1D",
            "symbol_id": "AAA",
            "structural_pass": False,
            "failing_rule_ids": ["b"],
            "failing_rule_values": {},
            "notes": "",
        },
    ]
    _write_structural_audit_artifacts(run_dir, rows, {"1D": {"a": 1, "b": 1}})
    with (run_dir / "structural_decisions.csv").open("r", encoding="utf-8") as fh:
        parsed = list(csv.DictReader(fh))
    assert [r["symbol_id"] for r in parsed] == ["AAA", "ZZZ"]


def test_training_decisions_csv_written_even_when_empty(tmp_path) -> None:
    run_dir = tmp_path / "run3"
    _write_training_selection_artifacts(
        run_dir,
        dynamic_thresholds={"1D": {"hard_fail": True}},
        population_stats={"1D": {"total_candidates": 0}},
        reason_counts_after={"1D": {}},
        decision_rows_csv=[],
    )
    with (run_dir / "decisions.csv").open("r", encoding="utf-8") as fh:
        parsed = list(csv.DictReader(fh))
    assert parsed == []


def test_salvage_never_selects_tail_kill_candidate() -> None:
    decisions = [
        {"candidate_id": "A", "structural_pass": True, "final_pass": False, "dynamic_pass": False, "fallback_selected": False, "composite_rank": 0.99, "reasons": []},
        {"candidate_id": "B", "structural_pass": True, "final_pass": False, "dynamic_pass": False, "fallback_selected": False, "composite_rank": 0.90, "reasons": []},
    ]
    killers = {
        "A": {"tail_kill_triggered": True},
        "B": {"tail_kill_triggered": False},
    }
    out = _apply_stage_salvage(
        decisions=decisions,
        stage_killers=killers,
        dynamic_enabled=True,
        structural_pass_count=2,
        valid_metric_candidates=2,
    )
    assert out["salvage_used"] is False
    assert out["hard_fail_reason"] == "tail_kill_switch_dominant_empty_selection"


def test_n1_tail_kill_cannot_be_salvaged() -> None:
    decisions = [
        {
            "candidate_id": "ONE",
            "structural_pass": True,
            "final_pass": False,
            "dynamic_pass": False,
            "fallback_selected": False,
            "composite_rank": 0.9,
            "reasons": [],
        }
    ]
    killers = {"ONE": {"tail_kill_triggered": True}}
    out = _apply_stage_salvage(
        decisions=decisions,
        stage_killers=killers,
        dynamic_enabled=True,
        structural_pass_count=1,
        valid_metric_candidates=1,
    )
    assert out["salvage_used"] is False
    assert out["hard_fail_reason"] == "tail_kill_switch_dominant_empty_selection"


def test_salvage_deterministic_tie_breaker_candidate_id() -> None:
    decisions = [
        {"candidate_id": "B", "structural_pass": True, "final_pass": False, "dynamic_pass": False, "fallback_selected": False, "composite_rank": 0.8, "reasons": []},
        {"candidate_id": "A", "structural_pass": True, "final_pass": False, "dynamic_pass": False, "fallback_selected": False, "composite_rank": 0.8, "reasons": []},
    ]
    out = _apply_stage_salvage(
        decisions=decisions,
        stage_killers={"A": {"tail_kill_triggered": False}, "B": {"tail_kill_triggered": False}},
        dynamic_enabled=True,
        structural_pass_count=2,
        valid_metric_candidates=2,
    )
    assert out["salvage_used"] is True
    assert out["salvage_selected"] == ["A"]


def test_stage_progress_artifact_written_even_on_failure_like_path(tmp_path) -> None:
    run_dir = tmp_path / "run_progress"
    _append_stage_progress(
        run_dir,
        tf="1D",
        step="prepare_filters",
        event="start",
        elapsed_s=0.0,
        counts={"discovered": 10},
    )
    _append_stage_progress(
        run_dir,
        tf="1D",
        step="prepare_filters",
        event="empty",
        elapsed_s=0.12,
        counts={"after_global_gate": 0},
    )
    text = (run_dir / "stage_progress.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(text) == 2


def test_stage_filter_report_counts_deterministic(tmp_path) -> None:
    run_dir = tmp_path / "run_filter"
    report = {"1D": {"discovered": 3, "after_data_quality": 2, "after_global_gate": 1}}
    sample = {"1D": {"discovered": _first20_symbols(["BBB", "AAA", "CCC"])}}
    _write_stage_filter_state(run_dir, stage_filter_report=report, stage_symbols_sample=sample)
    data = json.loads((run_dir / "stage_filter_report.json").read_text(encoding="utf-8"))
    snap = json.loads((run_dir / "stage_symbols_sample.json").read_text(encoding="utf-8"))
    assert data["1D"]["after_global_gate"] == 1
    assert snap["1D"]["discovered"] == ["AAA", "BBB", "CCC"]


def test_debug_one_symbol_scope_is_deterministic() -> None:
    symbols = ["ZZZ", "ABM", "AAA", "ABM"]
    scoped = _debug_symbol_scope(symbols, one_symbol="ABM", max_internal_steps=10)
    assert scoped == ["ABM", "ABM"]
    capped = _debug_symbol_scope(symbols, one_symbol="", max_internal_steps=2)
    assert capped == ["AAA", "ABM"]


def test_per_symbol_progress_artifact_written(tmp_path) -> None:
    pdir = tmp_path / "per_symbol" / "ABM" / "1D"
    _emit_symbol_step(
        pdir,
        tf="1D",
        symbol="ABM",
        step="run_cascade_training",
        event="start",
        elapsed_s=0.0,
        details={"timeout_s": 10},
    )
    lines = (pdir / "train_step_progress.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1


def test_tf_precheck_audit_artifacts_written(tmp_path) -> None:
    run_dir = tmp_path / "run_tf_audit"
    dq = {
        ("AAA", "1H"): GateDecision(
            symbol="AAA",
            timeframe="1H",
            stage="data_quality",
            status="FAIL",
            reason="spacing_not_timeframe_like",
            details={"path": "raw/Stock_parquet/AAA_1H.parquet", "match_frac": 0.8, "expected_s": 3600},
        )
    }
    _write_tf_precheck_audit(
        run_dir,
        tf="1H",
        symbols=["AAA"],
        passed_symbols=[],
        dq_by_symbol_tf=dq,
        global_decisions={},
    )
    tf_dir = run_dir / "tf_audit"
    assert (tf_dir / "1H_precheck_reason_counts.json").exists()
    assert (tf_dir / "1H_precheck_autopsy_first20.json").exists()
    assert (tf_dir / "1H_data_contract_report.json").exists()


def test_training_subset_selection_deterministic_history_ranked() -> None:
    syms = ["ZZZ", "AAA", "BBB", "AAA"]
    rank = {"AAA": 2, "BBB": 0, "ZZZ": 1}
    ordered = _deterministic_train_order(syms, universe_rank=rank, method="history_ranked")
    assert ordered == ["BBB", "ZZZ", "AAA"]
    selected, skipped = _select_training_subset(
        syms,
        max_train_symbols_per_tf=2,
        universe_rank=rank,
        selection_method="history_ranked",
    )
    assert selected == ["BBB", "ZZZ"]
    assert skipped == ["AAA"]


def test_load_training_budget_cfg_defaults_from_stage_timeouts() -> None:
    cfg = {"training_budget": {"enabled": True, "max_train_symbols_per_tf": 3}}
    loaded = _load_training_budget_cfg(cfg, {"training_symbol": 111, "training_loop": 222})
    assert loaded["enabled"] is True
    assert loaded["max_train_symbols_per_tf"] == 3
    assert loaded["per_symbol_timeout_s"] == 111
    assert loaded["stage_timeout_s"] == 222
    assert loaded["decouple_tf_pool_from_prev_tf"] is True


def test_load_training_budget_cfg_supports_per_tf_dict() -> None:
    cfg = {
        "training_budget": {
            "enabled": True,
            "max_train_symbols_per_tf": {"1D": 2, "1H": 3, "30M": 2, "5M": 1, "1M": 0},
        }
    }
    loaded = _load_training_budget_cfg(cfg, {"training_symbol": 100, "training_loop": 200})
    assert isinstance(loaded["max_train_symbols_per_tf"], dict)
    assert loaded["max_train_symbols_per_tf"]["1D"] == 2
    assert loaded["max_train_symbols_per_tf"]["1M"] == 0


def test_load_training_budget_cfg_fast_smoke_caps_and_enables() -> None:
    cfg = {
        "training_budget": {
            "enabled": False,
            "max_train_symbols_per_tf": {"1D": 3, "1H": 2, "1M": 0},
            "per_symbol_timeout_s": 900,
            "stage_timeout_s": 2400,
        }
    }
    loaded = _load_training_budget_cfg(
        cfg,
        {"training_symbol": 500, "training_loop": 2000},
        runtime_profile="fast_smoke",
    )
    assert loaded["enabled"] is True
    assert loaded["max_train_symbols_per_tf"]["1D"] == 1
    assert loaded["max_train_symbols_per_tf"]["1H"] == 1
    assert loaded["max_train_symbols_per_tf"]["1M"] == 0
    assert loaded["per_symbol_timeout_s"] == 120
    assert loaded["stage_timeout_s"] == 600
    assert loaded["runtime_profile"] == "fast_smoke"


def test_load_training_budget_cfg_fast_smoke_preserves_stricter_limits() -> None:
    cfg = {
        "training_budget": {
            "enabled": True,
            "max_train_symbols_per_tf": 1,
            "per_symbol_timeout_s": 60,
            "stage_timeout_s": 300,
        }
    }
    loaded = _load_training_budget_cfg(
        cfg,
        {"training_symbol": 100, "training_loop": 200},
        runtime_profile="fast_smoke",
    )
    assert loaded["max_train_symbols_per_tf"] == 1
    assert loaded["per_symbol_timeout_s"] == 60
    assert loaded["stage_timeout_s"] == 300


def test_load_training_budget_cfg_smoke_plus_caps_and_enables() -> None:
    cfg = {
        "training_budget": {
            "enabled": False,
            "max_train_symbols_per_tf": {"1D": 4, "1H": 2, "1M": 0},
            "per_symbol_timeout_s": 900,
            "stage_timeout_s": 2400,
        }
    }
    loaded = _load_training_budget_cfg(
        cfg,
        {"training_symbol": 500, "training_loop": 2000},
        runtime_profile="smoke_plus",
    )
    assert loaded["enabled"] is True
    assert loaded["max_train_symbols_per_tf"]["1D"] == 1
    assert loaded["max_train_symbols_per_tf"]["1H"] == 1
    assert loaded["max_train_symbols_per_tf"]["1M"] == 0
    assert loaded["per_symbol_timeout_s"] == 360
    assert loaded["stage_timeout_s"] == 1200
    assert loaded["runtime_profile"] == "smoke_plus"


def test_load_training_budget_cfg_smoke_plus_preserves_stricter_limits() -> None:
    cfg = {
        "training_budget": {
            "enabled": True,
            "max_train_symbols_per_tf": 1,
            "per_symbol_timeout_s": 180,
            "stage_timeout_s": 800,
        }
    }
    loaded = _load_training_budget_cfg(
        cfg,
        {"training_symbol": 500, "training_loop": 2000},
        runtime_profile="smoke_plus",
    )
    assert loaded["max_train_symbols_per_tf"] == 1
    assert loaded["per_symbol_timeout_s"] == 180
    assert loaded["stage_timeout_s"] == 800


def test_resolve_max_train_cap_scalar_backward_compatible() -> None:
    cap, explicit = _resolve_max_train_cap_for_tf(max_train_symbols_per_tf=3, tf="1H")
    assert cap == 3
    assert explicit is False


def test_resolve_max_train_cap_dict_including_zero() -> None:
    cap, explicit = _resolve_max_train_cap_for_tf(
        max_train_symbols_per_tf={"1D": 2, "1H": 3, "1M": 0},
        tf="1M",
    )
    assert cap == 0
    assert explicit is True


def test_top_fail_reasons_is_sorted_deterministically() -> None:
    reasons = _top_fail_reasons({"final_pass": 1, "b": 2, "a": 2, "c": 1}, top_n=3)
    assert reasons == ["a:2", "b:2", "c:1"]


def test_should_continue_on_training_timeout_when_budget_enabled() -> None:
    assert _should_continue_on_training_timeout({"enabled": True}) is True
    assert _should_continue_on_training_timeout({"enabled": False}) is False


def test_fail_stage_empty_includes_timeframe_and_writes_stage_state(tmp_path) -> None:
    run_dir = tmp_path / "run_fail_stage"
    stage_state = _stage_state_init(tf="1H", discovered=10)
    stage_filter_report = {"1H": {"discovered": 10, "after_global_gate": 0}}
    stage_symbols_sample = {"1H": {"discovered": ["AAA"]}}
    try:
        _fail_stage_empty(
            tf="1H",
            step="after_global_gate",
            reason="no_symbols_after_precheck",
            run_dir=run_dir,
            stage_state=stage_state,
            stage_filter_report=stage_filter_report,
            stage_symbols_sample=stage_symbols_sample,
            precheck_audit={
                "symbols": ["AAA"],
                "passed_symbols": [],
                "dq_by_symbol_tf": {},
                "global_decisions": {},
            },
        )
        assert False, "expected SystemExit"
    except SystemExit as e:
        assert str(e) == "stage_empty_after:1H:after_global_gate:no_symbols_after_precheck"
    assert (run_dir / "stage_state" / "1H_stage_state.json").exists()
    assert (run_dir / "tf_audit" / "1H_precheck_reason_counts.json").exists()


def test_resolve_runtime_timeframes_accepts_default_set() -> None:
    out = _resolve_runtime_timeframes(
        {
            "timeframes": ["1D", "1H", "30M", "5M", "1M"],
            "cascade_order": ["1D", "1H", "30M", "5M", "1M"],
        }
    )
    assert out["ok"] is True
    assert out["unexpected_timeframe"] is None
    assert out["timeframes"] == ["1D", "1H", "30M", "5M", "1M"]
    assert out["cascade_order"] == ["1D", "1H", "30M", "5M", "1M"]


def test_resolve_runtime_timeframes_rejects_unexpected_tf() -> None:
    out = _resolve_runtime_timeframes(
        {
            "timeframes": ["1D", "2H"],
            "cascade_order": ["1D", "2H"],
        }
    )
    assert out["ok"] is False
    assert out["reason"] == "config_invalid:unexpected_timeframe:2H"


def test_resolve_runtime_timeframes_allows_explicit_extension() -> None:
    out = _resolve_runtime_timeframes(
        {
            "timeframes": ["1D", "2H"],
            "cascade_order": ["1D", "2H"],
            "allowed_timeframes": ["2H"],
        }
    )
    assert out["ok"] is True
    assert "2H" in out["allowed_timeframes"]


def test_resolve_stage_pool_decouples_1h_from_prev_promotions() -> None:
    source, pool = _resolve_stage_pool(
        tf="1H",
        eligible_symbols=["ABT"],
        gg_pass_symbols=["ABT", "AJG", "AZO"],
        decouple_tf_pool_from_prev_tf=True,
    )
    assert source == "tf_precheck_pass"
    assert sorted(pool) == ["ABT", "AJG", "AZO"]


def test_resolve_stage_pool_uses_prev_promotions_when_decouple_disabled() -> None:
    source, pool = _resolve_stage_pool(
        tf="1H",
        eligible_symbols=["ABT"],
        gg_pass_symbols=["ABT", "AJG", "AZO"],
        decouple_tf_pool_from_prev_tf=False,
    )
    assert source == "prev_tf_promoted"
    assert sorted(pool) == ["ABT"]


def test_dynamic_hard_fail_reason_normalization() -> None:
    assert (
        _normalize_dynamic_hard_fail_reason(
            current_reason="no_structural_pass_candidates",
            structural_pass_count=1,
            stage_candidates_count=1,
        )
        == "no_dynamic_gate_input_candidates"
    )
    assert (
        _normalize_dynamic_hard_fail_reason(
            current_reason="",
            structural_pass_count=0,
            stage_candidates_count=0,
        )
        == "no_structural_pass_candidates"
    )
    assert (
        _normalize_dynamic_hard_fail_reason(
            current_reason="",
            structural_pass_count=2,
            stage_candidates_count=0,
        )
        == "no_stage_candidates_built"
    )


def test_stage_state_written_and_invariant_violation_detected(tmp_path) -> None:
    run_dir = tmp_path / "run_state"
    st = _stage_state_init(tf="1H", discovered=10)
    st["structural_pass"] = 0
    st["after_training_setup"] = 1
    st["stage_candidates_built"] = 1
    st["candidates_built"] = 1
    _write_stage_state(run_dir, tf="1H", state=st)
    assert (run_dir / "stage_state" / "1H_stage_state.json").exists()
    errs = _check_stage_invariants(state=st, stage_decisions_added=1)
    assert "after_training_setup_gt0_requires_structural_pass_gt0" in errs


def test_oos_window_alignment_non_overlapping() -> None:
    assert _is_non_overlapping_window(
        "2024-01-01T00:00:00+00:00",
        "2024-01-10T00:00:00+00:00",
        "2024-01-11T00:00:00+00:00",
        "2024-01-15T00:00:00+00:00",
    )
    assert not _is_non_overlapping_window(
        "2024-01-01T00:00:00+00:00",
        "2024-01-10T00:00:00+00:00",
        "2024-01-10T00:00:00+00:00",
        "2024-01-15T00:00:00+00:00",
    )


def test_oos_autopsy_artifact_written_when_triggered(tmp_path) -> None:
    run_dir = tmp_path / "run_oos"
    stage_by_symbol = {
        "ABT": {
            "metrics_bundle": {
                "metrics": {
                    "sharpe": 1.2,
                    "sharpe_is_mean": 2.0,
                    "sharpe_oos_over_is": 0.6,
                    "fold_metrics": [
                        {
                            "sharpe": 1.2,
                            "sharpe_is": 2.0,
                            "is_start": "2024-01-01T00:00:00+00:00",
                            "is_end": "2024-01-10T00:00:00+00:00",
                            "oos_start": "2024-01-11T00:00:00+00:00",
                            "oos_end": "2024-01-15T00:00:00+00:00",
                            "is_ret_count": 10,
                            "oos_ret_count": 5,
                            "is_ret_mean": 0.001,
                            "oos_ret_mean": 0.0005,
                            "is_ret_std": 0.01,
                            "oos_ret_std": 0.02,
                        }
                    ],
                },
                "gate": {
                    "diagnostics": [
                        {
                            "name": "sharpe_oos_over_is",
                            "value": 0.6,
                            "threshold": 0.7,
                            "op": ">=",
                            "reason": "is_oos_degradation",
                        }
                    ]
                },
            }
        }
    }
    stage_killers = {"ABT": {"oos_degradation_triggered": True}}
    _write_oos_degradation_autopsy(run_dir, tf="1H", stage_by_symbol=stage_by_symbol, stage_killers=stage_killers)
    out = run_dir / "tf_audit" / "1H_oos_degradation_autopsy.json"
    assert out.exists()
