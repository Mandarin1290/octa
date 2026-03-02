from __future__ import annotations

import argparse
import csv
import json
import math
import os
import pickle
import tempfile
import traceback
import multiprocessing as mp
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

from octa.core.governance.immutability_guard import assert_write_allowed, is_production_context
from octa.core.governance.model_registry import (
    append_entry as append_model_registry_entry,
    build_registry_entry,
    compute_deps_fingerprint,
)
from octa.core.gates.training_selection_gate import (
    MetricRule,
    TrainingSelectionGateConfig,
    evaluate_training_selection,
)
from octa_ops.autopilot.budgets import BudgetExceeded, ResourceBudgetController
from octa_ops.autopilot.cascade_train import (
    CascadePolicy,
    run_cascade_training,
    write_gate_matrix,
)
from octa_ops.autopilot.data_quality import (
    DataQualityPolicy,
    evaluate_data_quality,
    write_quality_outputs,
)
from octa_ops.autopilot.global_gate import (
    GlobalGatePolicy,
    evaluate_global_gate,
    write_global_outputs,
)
from octa_ops.autopilot.paper_runner import run_paper
from octa_ops.autopilot.registry import ArtifactRegistry
from octa_ops.autopilot.types import GateDecision, normalize_timeframe, now_utc_iso, timeframe_seconds
from octa_ops.autopilot.universe import discover_universe
from octa.support.branding import (
    BRAND_NAME,
    PLATFORM_NAME,
    TAGLINE,
    print_banner_once,
)


class _StepTimeoutError(Exception):
    pass


def _load_yaml(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}


def _policy_execution_flags(policy_path: str) -> Dict[str, Any]:
    p = Path(policy_path)
    if not p.exists():
        return {}
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    safety = raw.get("safety") if isinstance(raw.get("safety"), dict) else {}
    return {
        "default_execution_enabled": bool(safety.get("default_execution_enabled", False)),
        "require_blessed_1d_1h": bool(safety.get("require_blessed_1d_1h", False)),
    }


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _relpath_or_abs(path: str, *, start: str = ".") -> str:
    try:
        return str(Path(path).resolve().relative_to(Path(start).resolve()))
    except Exception:
        return str(path)


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def _load_dynamic_gate_cfg(raw_cfg: Dict[str, Any]) -> tuple[TrainingSelectionGateConfig, Dict[str, Any]]:
    configured = isinstance(raw_cfg.get("dynamic_gate"), dict)
    dg = raw_cfg.get("dynamic_gate", {}) if configured else {}
    defaults_applied: list[str] = []
    validation_errors: list[str] = []
    metrics_raw = dg.get("metrics", {}) if isinstance(dg.get("metrics"), dict) else {}
    metric_alias = {"pf": "profit_factor", "max_dd": "max_drawdown"}
    enabled = bool(dg.get("enabled", False))
    if not configured:
        defaults_applied.append("dynamic_gate_section_missing")
    metric_rules: Dict[str, MetricRule] = {}
    for key, value in metrics_raw.items():
        out_key = metric_alias.get(str(key), str(key))
        if not isinstance(value, dict):
            if enabled:
                validation_errors.append(f"invalid_metric_rule:{out_key}")
            continue
        direction = str(value.get("direction", "high")).lower()
        if direction not in {"high", "low"}:
            if enabled:
                validation_errors.append(f"invalid_metric_direction:{out_key}:{direction}")
            direction = "high"
        try:
            weight = float(value.get("weight", 0.0))
        except Exception:
            if enabled:
                validation_errors.append(f"invalid_metric_weight:{out_key}")
            weight = 0.0
        if enabled and (not math.isfinite(weight) or weight <= 0.0):
            validation_errors.append(f"non_positive_metric_weight:{out_key}")
        metric_rules[str(out_key)] = MetricRule(direction=direction, weight=weight)
    required_metrics = dg.get("required_metrics")
    if isinstance(required_metrics, list) and required_metrics:
        req = tuple(metric_alias.get(str(x), str(x)) for x in required_metrics if str(x).strip())
    else:
        if enabled:
            validation_errors.append("missing_required_metrics")
        req = tuple(metric_rules.keys()) if metric_rules else ("sharpe", "profit_factor", "calmar", "max_drawdown")
        defaults_applied.append("required_metrics")

    method = str(dg.get("method", "percentile_rank"))
    if "method" not in dg:
        defaults_applied.append("method")
    min_population = int(dg.get("min_population", 30))
    if "min_population" not in dg:
        defaults_applied.append("min_population")
    top_percentile = float(dg.get("top_percentile", 0.70))
    if "top_percentile" not in dg:
        defaults_applied.append("top_percentile")
    fallback_top_k_if_empty = int(dg.get("fallback_top_k_if_empty", 1))
    if "fallback_top_k_if_empty" not in dg:
        defaults_applied.append("fallback_top_k_if_empty")

    if enabled:
        if method != "percentile_rank":
            validation_errors.append(f"invalid_method:{method}")
        if min_population < 1:
            validation_errors.append("invalid_min_population")
        if top_percentile < 0.0 or top_percentile > 1.0:
            validation_errors.append("invalid_top_percentile")
        if fallback_top_k_if_empty < 0:
            validation_errors.append("invalid_fallback_top_k_if_empty")
        if "metrics" not in dg or not isinstance(dg.get("metrics"), dict):
            validation_errors.append("missing_metrics")
        for metric_name in req:
            if metric_name not in metric_rules:
                validation_errors.append(f"missing_metric_rule:{metric_name}")

    resolved = TrainingSelectionGateConfig(
        enabled=enabled,
        method=method,
        min_population=min_population,
        top_percentile=top_percentile,
        fallback_top_k_if_empty=fallback_top_k_if_empty,
        required_metrics=req,
        metrics=metric_rules if metric_rules else None,
    )
    snapshot = {
        "configured": configured,
        "enabled_requested": enabled,
        "raw": dg,
        "resolved": {
            "enabled": bool(resolved.enabled),
            "method": str(resolved.method),
            "min_population": int(resolved.min_population),
            "top_percentile": float(resolved.top_percentile),
            "fallback_top_k_if_empty": int(resolved.fallback_top_k_if_empty),
            "required_metrics": list(resolved.required_metrics),
            "metrics": {
                str(k): {"direction": str(v.direction), "weight": float(v.weight)}
                for k, v in (resolved.metrics or {}).items()
            },
        },
        "defaults_applied": sorted(dict.fromkeys(defaults_applied)),
        "validation_errors": sorted(dict.fromkeys(validation_errors)),
    }
    return resolved, snapshot


def _write_resolved_config_snapshot(run_dir: Path, update: Dict[str, Any]) -> None:
    path = run_dir / "resolved_config_snapshot.json"
    payload: Dict[str, Any] = {}
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = loaded
        except Exception:
            payload = {}
    payload.update(update)
    _write_json(path, payload)


def _write_dynamic_gate_evidence(run_dir: Path, snapshot: Dict[str, Any], status: Dict[str, Any]) -> None:
    _write_resolved_config_snapshot(run_dir, {"dynamic_gate": snapshot})
    _write_json(run_dir / "dynamic_gate_status.json", status)


def _write_structural_audit_artifacts(
    run_dir: Path,
    rows: list[Dict[str, Any]],
    reason_counts_by_tf: Dict[str, Dict[str, int]],
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    ordered_rows = sorted(rows, key=lambda r: (str(r.get("timeframe")), str(r.get("symbol_id"))))
    csv_path = run_dir / "structural_decisions.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        fieldnames = [
            "timeframe",
            "symbol_id",
            "structural_pass",
            "failing_rule_ids",
            "failing_rule_values",
            "notes",
        ]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in ordered_rows:
            writer.writerow(
                {
                    "timeframe": row.get("timeframe"),
                    "symbol_id": row.get("symbol_id"),
                    "structural_pass": bool(row.get("structural_pass", False)),
                    "failing_rule_ids": json.dumps(row.get("failing_rule_ids", []), ensure_ascii=False, sort_keys=True),
                    "failing_rule_values": json.dumps(row.get("failing_rule_values", {}), ensure_ascii=False, sort_keys=True, default=str),
                    "notes": str(row.get("notes", "")),
                }
            )

    overall = Counter()
    for tf_map in reason_counts_by_tf.values():
        for k, v in tf_map.items():
            overall[str(k)] += int(v)
    _write_json(
        run_dir / "structural_reason_counts.json",
        {
            "overall": {k: int(v) for k, v in sorted(overall.items(), key=lambda kv: kv[0])},
            "by_timeframe": {
                str(tf): {k: int(v) for k, v in sorted(dict(rc).items(), key=lambda kv: kv[0])}
                for tf, rc in sorted(reason_counts_by_tf.items(), key=lambda kv: kv[0])
            },
        },
    )

    failed_first20 = [
        {
            "timeframe": r.get("timeframe"),
            "symbol_id": r.get("symbol_id"),
            "failing_rule_ids": r.get("failing_rule_ids", []),
            "failing_rule_values": r.get("failing_rule_values", {}),
            "notes": r.get("notes", ""),
        }
        for r in ordered_rows
        if not bool(r.get("structural_pass", False))
    ][:20]
    _write_json(run_dir / "structural_autopsy_first20.json", {"rows": failed_first20})

    total = len(ordered_rows)
    passed = sum(1 for r in ordered_rows if bool(r.get("structural_pass", False)))
    failed = total - passed
    summary = (
        "# Structural Gate Summary\n\n"
        f"- evaluated_rows: {total}\n"
        f"- structural_pass: {passed}\n"
        f"- structural_fail: {failed}\n"
        f"- autopsy_first20: {len(failed_first20)}\n"
    )
    (run_dir / "structural_gate_summary.md").write_text(summary, encoding="utf-8")


def _write_training_selection_artifacts(
    run_dir: Path,
    dynamic_thresholds: Dict[str, Any],
    population_stats: Dict[str, Any],
    reason_counts_after: Dict[str, Dict[str, int]],
    decision_rows_csv: list[Dict[str, Any]],
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "dynamic_thresholds.json").write_text(
        json.dumps(dynamic_thresholds, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (run_dir / "population_stats.json").write_text(
        json.dumps(population_stats, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (run_dir / "reason_counts_after.json").write_text(
        json.dumps(reason_counts_after, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    with (run_dir / "decisions.csv").open("w", encoding="utf-8", newline="") as fh:
        fieldnames = [
            "timeframe",
            "candidate_id",
            "metrics_json",
            "sharpe",
            "profit_factor",
            "calmar",
            "max_drawdown",
            "population_n",
            "structural_pass",
            "static_pass",
            "dynamic_pass",
            "final_pass",
            "fallback_selected",
            "fallback_used",
            "salvage_used",
            "composite_rank",
            "reject_reason_code",
            "reasons",
        ]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in sorted(decision_rows_csv, key=lambda r: (str(r.get("timeframe")), str(r.get("candidate_id")))):
            writer.writerow(row)


def _append_stage_progress(
    run_dir: Path,
    *,
    tf: str,
    step: str,
    event: str,
    elapsed_s: float,
    counts: Dict[str, Any] | None = None,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": now_utc_iso(),
        "tf": str(tf),
        "step": str(step),
        "event": str(event),
        "elapsed_s": float(elapsed_s),
        "counts": dict(counts or {}),
    }
    with (run_dir / "stage_progress.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def _first20_symbols(symbols: list[str]) -> list[str]:
    return [str(s) for s in sorted([str(x) for x in symbols])[:20]]


def _write_stage_filter_state(
    run_dir: Path,
    *,
    stage_filter_report: Dict[str, Any],
    stage_symbols_sample: Dict[str, Any],
) -> None:
    _write_json(run_dir / "stage_filter_report.json", stage_filter_report)
    _write_json(run_dir / "stage_symbols_sample.json", stage_symbols_sample)


def _stage_state_init(*, tf: str, discovered: int) -> Dict[str, Any]:
    return {
        "tf": str(tf),
        "discovered": int(discovered),
        "after_data_quality": 0,
        "after_global_gate": 0,
        "structural_pass": 0,
        "after_pretrain_precheck": 0,
        "pool_source": None,
        "pool_size_before_budget": 0,
        "selected_for_training": 0,
        "selected_for_training_list": [],
        "trained_completed": 0,
        "stage_candidates_built": 0,
        "dynamic_gate_input_count": 0,
        "dynamic_gate_final_pass_count": 0,
        "top_fail_reasons": [],
        "status": "stage_started",
        "reason": None,
    }


def _write_stage_state(run_dir: Path, *, tf: str, state: Dict[str, Any]) -> None:
    out = run_dir / "stage_state"
    out.mkdir(parents=True, exist_ok=True)
    _write_json(out / f"{normalize_timeframe(tf)}_stage_state.json", state)


def _fail_stage_empty(
    *,
    tf: str,
    step: str,
    reason: str,
    run_dir: Path,
    stage_state: Dict[str, Any],
    stage_filter_report: Dict[str, Any],
    stage_symbols_sample: Dict[str, Any],
    precheck_audit: Dict[str, Any] | None = None,
) -> None:
    tf_norm = normalize_timeframe(tf)
    if isinstance(precheck_audit, dict):
        _write_tf_precheck_audit(
            run_dir,
            tf=str(tf_norm),
            symbols=[str(x) for x in (precheck_audit.get("symbols") or [])],
            passed_symbols=[str(x) for x in (precheck_audit.get("passed_symbols") or [])],
            dq_by_symbol_tf=precheck_audit.get("dq_by_symbol_tf") or {},
            global_decisions=precheck_audit.get("global_decisions") or {},
        )
    _write_stage_filter_state(run_dir, stage_filter_report=stage_filter_report, stage_symbols_sample=stage_symbols_sample)
    stage_state["status"] = "hard_fail"
    stage_state["reason"] = f"stage_empty_after:{tf_norm}:{step}:{reason}"
    _write_stage_state(run_dir, tf=str(tf_norm), state=stage_state)
    raise SystemExit(f"stage_empty_after:{tf_norm}:{step}:{reason}")


def _check_stage_invariants(*, state: Dict[str, Any], stage_decisions_added: int) -> list[str]:
    errs: list[str] = []
    if int(state.get("after_training_setup", state.get("trained_completed", 0)) or 0) > 0 and int(state.get("structural_pass", 0) or 0) <= 0:
        errs.append("after_training_setup_gt0_requires_structural_pass_gt0")
    if int(state.get("candidates_built", state.get("stage_candidates_built", 0)) or 0) > 0 and int(state.get("stage_candidates_built", 0) or 0) <= 0:
        errs.append("candidates_built_gt0_requires_stage_candidates_gt0")
    if int(stage_decisions_added) > 0 and int(state.get("stage_candidates_built", 0) or 0) <= 0:
        errs.append("stage_decisions_gt0_requires_stage_candidates_gt0")
    return errs


def _normalize_dynamic_hard_fail_reason(
    *,
    current_reason: str | None,
    structural_pass_count: int,
    stage_candidates_count: int,
) -> str:
    if int(structural_pass_count) <= 0:
        return "no_structural_pass_candidates"
    if int(stage_candidates_count) <= 0:
        return "no_stage_candidates_built"
    cur = str(current_reason or "").strip()
    if cur in {"", "no_structural_pass_candidates"}:
        return "no_dynamic_gate_input_candidates"
    return cur


def _write_tf_precheck_audit(
    run_dir: Path,
    *,
    tf: str,
    symbols: list[str],
    passed_symbols: list[str],
    dq_by_symbol_tf: Dict[tuple[str, str], GateDecision],
    global_decisions: Dict[str, GateDecision],
) -> None:
    tf_norm = str(normalize_timeframe(tf))
    tf_dir = run_dir / "tf_audit"
    tf_dir.mkdir(parents=True, exist_ok=True)
    passed_set = {str(s) for s in passed_symbols}
    rows: list[Dict[str, Any]] = []
    reason_counter: Counter[str] = Counter()
    for symbol in sorted([str(s) for s in symbols]):
        dq = dq_by_symbol_tf.get((symbol, tf_norm))
        gg = global_decisions.get(symbol) if tf_norm == "1D" else None
        reasons: list[str] = []
        dq_details = dq.details if dq and isinstance(dq.details, dict) else {}
        if dq is None:
            reasons.append("data_quality:missing_decision")
        elif str(dq.status) != "PASS":
            reasons.append(f"data_quality:{str(dq.reason or 'unknown')}")
        if tf_norm == "1D":
            if gg is None:
                reasons.append("global_gate:missing_decision")
            elif str(gg.status) != "PASS":
                reasons.append(f"global_gate:{str(gg.reason or 'unknown')}")
        precheck_pass = symbol in passed_set
        if not precheck_pass:
            if reasons:
                for r in reasons:
                    reason_counter[str(r)] += 1
            else:
                reason_counter["unknown_precheck_elimination"] += 1
        rows.append(
            {
                "symbol": symbol,
                "timeframe": tf_norm,
                "precheck_pass": bool(precheck_pass),
                "reasons": sorted(dict.fromkeys(reasons)),
                "data_contract": {
                    "path": dq_details.get("path"),
                    "n": dq_details.get("n"),
                    "dup_frac": dq_details.get("dup_frac"),
                    "expected_s": dq_details.get("expected_s"),
                    "match_frac": dq_details.get("match_frac"),
                    "median_s": dq_details.get("median_s"),
                    "p90_s": dq_details.get("p90_s"),
                    "compared_deltas_n": dq_details.get("compared_deltas_n"),
                    "ignored_session_gap_n": dq_details.get("ignored_session_gap_n"),
                },
            }
        )

    _write_json(
        tf_dir / f"{tf_norm}_precheck_reason_counts.json",
        {
            "timeframe": tf_norm,
            "symbols_total": int(len(rows)),
            "symbols_pass": int(len(passed_set)),
            "symbols_fail": int(len(rows) - len(passed_set)),
            "reason_counts": {k: int(v) for k, v in sorted(reason_counter.items(), key=lambda kv: kv[0])},
        },
    )
    failed_rows = [r for r in rows if not bool(r.get("precheck_pass", False))]
    _write_json(
        tf_dir / f"{tf_norm}_precheck_autopsy_first20.json",
        {
            "timeframe": tf_norm,
            "rows": failed_rows[:20],
        },
    )
    _write_json(
        tf_dir / f"{tf_norm}_data_contract_report.json",
        {
            "timeframe": tf_norm,
            "expected_seconds": timeframe_seconds(tf_norm),
            "rows_total": int(len(rows)),
            "rows_pass": int(len(passed_set)),
            "sample_first20": rows[:20],
        },
    )


def _stage_timeouts(cfg: Dict[str, Any]) -> Dict[str, int]:
    raw = cfg.get("stage_runtime") if isinstance(cfg.get("stage_runtime"), dict) else {}
    return {
        "prepare_filters": int(raw.get("prepare_filters", 300)),
        "structural_precheck": int(raw.get("structural_precheck", 300)),
        "training_loop": int(raw.get("training_loop", 1200)),
        "training_symbol": int(raw.get("training_symbol", 300)),
        "selection_eval": int(raw.get("selection_eval", 300)),
    }


def _run_cascade_training_with_timeout(*, timeout_s: int, kwargs: Dict[str, Any]) -> Any:
    if int(timeout_s) <= 0:
        raise _StepTimeoutError("timeout_before_symbol_start")
    proc: Any = None
    result_path: str | None = None
    try:
        if "fork" not in mp.get_all_start_methods():
            return run_cascade_training(**kwargs)
        ctx = mp.get_context("fork")
        with tempfile.NamedTemporaryFile(prefix="octa_train_worker_", suffix=".pkl", delete=False) as tfh:
            result_path = str(tfh.name)
        proc = ctx.Process(target=_run_cascade_training_worker, args=(str(result_path), kwargs))
        proc.start()
        proc.join(float(timeout_s))
        if proc.is_alive():
            proc.terminate()
            proc.join(5.0)
            raise _StepTimeoutError("symbol_training_timeout")
        payload = None
        if result_path and Path(result_path).exists():
            with Path(result_path).open("rb") as fh:
                payload = pickle.load(fh)
        if not isinstance(payload, dict):
            raise RuntimeError("training_worker_no_payload")
        if bool(payload.get("ok", False)):
            return payload.get("result")
        raise RuntimeError(
            f"training_worker_exception:{payload.get('exc_type')}:{payload.get('error')}"
        )
    finally:
        try:
            if proc is not None and proc.is_alive():
                proc.terminate()
                proc.join(2.0)
        except Exception:
            pass
        try:
            if result_path and Path(result_path).exists():
                os.unlink(result_path)
        except Exception:
            pass


def _run_cascade_training_worker(result_path: str, worker_kwargs: Dict[str, Any]) -> None:
    payload: Dict[str, Any]
    try:
        result = run_cascade_training(**worker_kwargs)
        payload = {"ok": True, "result": result}
    except Exception as exc:
        payload = {
            "ok": False,
            "error": str(exc),
            "exc_type": str(type(exc).__name__),
            "traceback": traceback.format_exc(),
        }
    with Path(result_path).open("wb") as fh:
        pickle.dump(payload, fh)


def _emit_symbol_step(
    per_symbol_dir: Path,
    *,
    tf: str,
    symbol: str,
    step: str,
    event: str,
    elapsed_s: float,
    details: Dict[str, Any] | None = None,
) -> None:
    payload = {
        "ts": now_utc_iso(),
        "symbol": str(symbol),
        "tf": str(tf),
        "step": str(step),
        "event": str(event),
        "elapsed_s": float(elapsed_s),
        "details": dict(details or {}),
    }
    _append_jsonl(per_symbol_dir / "train_step_progress.jsonl", payload)


def _debug_symbol_scope(symbols: list[str], *, one_symbol: str, max_internal_steps: int) -> list[str]:
    scoped = sorted([str(s) for s in symbols])
    if one_symbol:
        scoped = [s for s in scoped if s == str(one_symbol)]
    if int(max_internal_steps) > 0:
        scoped = scoped[: int(max_internal_steps)]
    return scoped


def _load_training_budget_cfg(
    raw_cfg: Dict[str, Any],
    step_budgets: Dict[str, int],
    *,
    runtime_profile: str = "default",
) -> Dict[str, Any]:
    tb = raw_cfg.get("training_budget", {}) if isinstance(raw_cfg.get("training_budget"), dict) else {}
    enabled = bool(tb.get("enabled", False))
    max_train_raw: Any = tb.get("max_train_symbols_per_tf", 2)
    if isinstance(max_train_raw, dict):
        max_train_norm = {normalize_timeframe(str(k)): int(v) for k, v in max_train_raw.items()}
    else:
        max_train_norm = int(max_train_raw)
    per_symbol_timeout_s = int(tb.get("per_symbol_timeout_s", int(step_budgets.get("training_symbol", 300))))
    stage_timeout_s = int(tb.get("stage_timeout_s", int(step_budgets.get("training_loop", 1200))))

    if str(runtime_profile) in {"fast_smoke", "smoke_plus"}:
        enabled = True
        if isinstance(max_train_norm, dict):
            max_train_norm = {str(k): min(int(v), 1) for k, v in max_train_norm.items()}
        else:
            max_train_norm = min(int(max_train_norm), 1)
        if str(runtime_profile) == "fast_smoke":
            per_symbol_timeout_cap = 120
            stage_timeout_cap = 600
        else:
            per_symbol_timeout_cap = 360
            stage_timeout_cap = 1200
        per_symbol_timeout_s = min(int(per_symbol_timeout_s), int(per_symbol_timeout_cap))
        stage_timeout_s = min(int(stage_timeout_s), int(stage_timeout_cap))

    return {
        "enabled": enabled,
        "max_train_symbols_per_tf": max_train_norm,
        "selection_method": str(tb.get("selection_method", "history_ranked")),
        "per_symbol_timeout_s": int(per_symbol_timeout_s),
        "stage_timeout_s": int(stage_timeout_s),
        "stop_after_first_candidate": bool(tb.get("stop_after_first_candidate", False)),
        "decouple_tf_pool_from_prev_tf": bool(tb.get("decouple_tf_pool_from_prev_tf", True)),
        "runtime_profile": str(runtime_profile),
    }


def _deterministic_train_order(symbols: list[str], *, universe_rank: Dict[str, int], method: str) -> list[str]:
    uniq = sorted(dict.fromkeys([str(s) for s in symbols]))
    if str(method) in {"history_ranked", "precheck_score_ranked"}:
        return sorted(uniq, key=lambda s: (int(universe_rank.get(str(s), 10**9)), str(s)))
    return sorted(uniq)


def _select_training_subset(
    symbols: list[str],
    *,
    max_train_symbols_per_tf: int,
    universe_rank: Dict[str, int],
    selection_method: str,
) -> tuple[list[str], list[str]]:
    ordered = _deterministic_train_order(symbols, universe_rank=universe_rank, method=selection_method)
    if int(max_train_symbols_per_tf) <= 0:
        return ordered, []
    selected = ordered[: int(max_train_symbols_per_tf)]
    skipped = ordered[int(max_train_symbols_per_tf):]
    return selected, skipped


def _resolve_stage_pool(
    *,
    tf: str,
    eligible_symbols: list[str],
    gg_pass_symbols: list[str],
    decouple_tf_pool_from_prev_tf: bool,
) -> tuple[str, set[str]]:
    tf_norm = normalize_timeframe(tf)
    if tf_norm == "1D":
        return "entry_1D_global_pass", set(str(s) for s in eligible_symbols)
    if bool(decouple_tf_pool_from_prev_tf):
        return "tf_precheck_pass", set(str(s) for s in gg_pass_symbols)
    return "prev_tf_promoted", set(str(s) for s in eligible_symbols)


def _resolve_max_train_cap_for_tf(
    *,
    max_train_symbols_per_tf: Any,
    tf: str,
) -> tuple[int, bool]:
    if isinstance(max_train_symbols_per_tf, dict):
        tf_norm = normalize_timeframe(tf)
        if tf_norm in max_train_symbols_per_tf:
            return int(max_train_symbols_per_tf[tf_norm]), True
        if "DEFAULT" in max_train_symbols_per_tf:
            return int(max_train_symbols_per_tf["DEFAULT"]), True
        return 0, True
    return int(max_train_symbols_per_tf), False


def _top_fail_reasons(reason_counts: Dict[str, Any], *, top_n: int = 5) -> list[str]:
    items: list[tuple[str, int]] = []
    for k, v in (reason_counts or {}).items():
        if str(k) == "final_pass":
            continue
        try:
            n = int(v)
        except Exception:
            continue
        if n > 0:
            items.append((str(k), n))
    items.sort(key=lambda kv: (-kv[1], kv[0]))
    return [f"{k}:{n}" for k, n in items[: int(top_n)]]


def _should_continue_on_training_timeout(training_budget_cfg: Dict[str, Any]) -> bool:
    return bool(training_budget_cfg.get("enabled", False))


def _extract_killer_autopsy(gate_dump: Dict[str, Any], reasons: list[str]) -> Dict[str, Any]:
    diagnostics = gate_dump.get("diagnostics", []) if isinstance(gate_dump, dict) and isinstance(gate_dump.get("diagnostics"), list) else []
    applied_thresholds = None
    for d in diagnostics:
        if isinstance(d, dict) and str(d.get("name")) == "applied_thresholds" and isinstance(d.get("value"), dict):
            applied_thresholds = d.get("value")
            break

    def _first_diag(name: str, reason: str) -> Dict[str, Any] | None:
        for d in diagnostics:
            if not isinstance(d, dict):
                continue
            if str(d.get("name")) == name or str(d.get("reason")) == reason:
                return d
        return None

    tail_diag = _first_diag("cvar_95_over_daily_vol", "tail_kill_switch")
    oos_diag = _first_diag("sharpe_oos_over_is", "is_oos_degradation")
    tail_trigger = any("tail_kill_switch" in str(r) for r in reasons) or (
        isinstance(tail_diag, dict) and bool(tail_diag.get("reason") == "tail_kill_switch")
    )
    oos_trigger = any("is_oos_degradation" in str(r) for r in reasons) or (
        isinstance(oos_diag, dict) and bool(oos_diag.get("reason") == "is_oos_degradation")
    )

    return {
        "tail_kill_triggered": bool(tail_trigger),
        "tail_inputs": {"metric": "cvar_95_over_daily_vol"},
        "tail_stats": {
            "value": (tail_diag or {}).get("value") if isinstance(tail_diag, dict) else None,
            "diagnostic_reason": (tail_diag or {}).get("reason") if isinstance(tail_diag, dict) else None,
        },
        "tail_thresholds": {
            "threshold": (tail_diag or {}).get("threshold") if isinstance(tail_diag, dict) else None,
            "op": (tail_diag or {}).get("op") if isinstance(tail_diag, dict) else "<=",
            "config": (applied_thresholds or {}).get("cvar_95_over_daily_vol_max") if isinstance(applied_thresholds, dict) else None,
        },
        "oos_degradation_triggered": bool(oos_trigger),
        "is_metrics": {
            "sharpe_oos_over_is": (oos_diag or {}).get("value") if isinstance(oos_diag, dict) else None,
        },
        "oos_metrics": {
            "diagnostic_reason": (oos_diag or {}).get("reason") if isinstance(oos_diag, dict) else None,
        },
        "degradation_calc": {
            "ratio": (oos_diag or {}).get("value") if isinstance(oos_diag, dict) else None,
        },
        "thresholds": {
            "sharpe_oos_over_is_min": (applied_thresholds or {}).get("sharpe_oos_over_is_min") if isinstance(applied_thresholds, dict) else None,
            "threshold": (oos_diag or {}).get("threshold") if isinstance(oos_diag, dict) else None,
            "op": (oos_diag or {}).get("op") if isinstance(oos_diag, dict) else ">=",
        },
    }


def _is_non_overlapping_window(is_start: Any, is_end: Any, oos_start: Any, oos_end: Any) -> bool:
    try:
        a0 = pd.Timestamp(str(is_start))
        a1 = pd.Timestamp(str(is_end))
        b0 = pd.Timestamp(str(oos_start))
        b1 = pd.Timestamp(str(oos_end))
        return bool(a1 < b0 and a0 <= a1 and b0 <= b1)
    except Exception:
        return False


def _write_oos_degradation_autopsy(
    run_dir: Path,
    *,
    tf: str,
    stage_by_symbol: Dict[str, Dict[str, Any]],
    stage_killers: Dict[str, Dict[str, Any]],
) -> None:
    tf_norm = str(normalize_timeframe(tf))
    tf_dir = run_dir / "tf_audit"
    tf_dir.mkdir(parents=True, exist_ok=True)
    rows: list[Dict[str, Any]] = []
    for symbol in sorted(stage_by_symbol.keys()):
        base = stage_by_symbol.get(symbol) or {}
        mb = base.get("metrics_bundle") if isinstance(base, dict) else {}
        if not isinstance(mb, dict):
            continue
        metrics = mb.get("metrics") if isinstance(mb.get("metrics"), dict) else {}
        gate = mb.get("gate") if isinstance(mb.get("gate"), dict) else {}
        kill = stage_killers.get(symbol) if isinstance(stage_killers.get(symbol), dict) else {}
        if not bool(kill.get("oos_degradation_triggered", False)):
            continue
        diags = gate.get("diagnostics") if isinstance(gate.get("diagnostics"), list) else []
        oos_diag = None
        for d in diags:
            if isinstance(d, dict) and (str(d.get("name")) == "sharpe_oos_over_is" or str(d.get("reason")) == "is_oos_degradation"):
                oos_diag = d
                break
        fold_rows: list[Dict[str, Any]] = []
        fold_metrics = metrics.get("fold_metrics") if isinstance(metrics.get("fold_metrics"), list) else []
        for fm in fold_metrics:
            if not isinstance(fm, dict):
                continue
            is_start = fm.get("is_start")
            is_end = fm.get("is_end")
            oos_start = fm.get("oos_start")
            oos_end = fm.get("oos_end")
            is_n = int(fm.get("is_ret_count", 0) or 0)
            oos_n = int(fm.get("oos_ret_count", 0) or 0)
            fold_rows.append(
                {
                    "is_window": {"start": is_start, "end": is_end},
                    "oos_window": {"start": oos_start, "end": oos_end},
                    "is_metrics": {"sharpe_is": fm.get("sharpe_is")},
                    "oos_metrics": {"sharpe_oos": fm.get("sharpe")},
                    "returns_summary": {
                        "is": {"count": is_n, "mean": fm.get("is_ret_mean"), "std": fm.get("is_ret_std")},
                        "oos": {"count": oos_n, "mean": fm.get("oos_ret_mean"), "std": fm.get("oos_ret_std")},
                    },
                    "alignment": {
                        "non_overlapping": _is_non_overlapping_window(is_start, is_end, oos_start, oos_end),
                        "enough_bars": bool(is_n >= 2 and oos_n >= 2),
                    },
                }
            )
        rows.append(
            {
                "symbol": str(symbol),
                "timeframe": tf_norm,
                "triggered": True,
                "rule": "is_oos_degradation",
                "formula": "sharpe_oos_over_is = sharpe_oos / sharpe_is_mean",
                "formula_outputs": {
                    "sharpe_oos": metrics.get("sharpe"),
                    "sharpe_is_mean": metrics.get("sharpe_is_mean"),
                    "sharpe_oos_over_is": metrics.get("sharpe_oos_over_is"),
                },
                "thresholds": {
                    "sharpe_oos_over_is_min": (oos_diag or {}).get("threshold") if isinstance(oos_diag, dict) else None,
                    "op": (oos_diag or {}).get("op") if isinstance(oos_diag, dict) else ">=",
                },
                "diagnostic_reason": (oos_diag or {}).get("reason") if isinstance(oos_diag, dict) else "is_oos_degradation",
                "windows": fold_rows,
                "window_alignment_summary": {
                    "folds": int(len(fold_rows)),
                    "all_non_overlapping": bool(fold_rows and all(bool((r.get("alignment") or {}).get("non_overlapping", False)) for r in fold_rows)),
                    "all_enough_bars": bool(fold_rows and all(bool((r.get("alignment") or {}).get("enough_bars", False)) for r in fold_rows)),
                },
            }
        )
    _write_json(tf_dir / f"{tf_norm}_oos_degradation_autopsy.json", {"timeframe": tf_norm, "rows": rows})


def _apply_stage_salvage(
    *,
    decisions: list[Dict[str, Any]],
    stage_killers: Dict[str, Dict[str, Any]],
    dynamic_enabled: bool,
    structural_pass_count: int,
    valid_metric_candidates: int,
) -> Dict[str, Any]:
    out = [dict(d) for d in decisions]
    final_pass_count = sum(1 for d in out if bool(d.get("final_pass", False)))
    tail_kill_count = sum(1 for v in stage_killers.values() if bool(v.get("tail_kill_triggered", False)))
    used = False
    selected: list[str] = []
    hard_fail_reason: str | None = None

    if (
        bool(dynamic_enabled)
        and final_pass_count == 0
        and int(tail_kill_count) == 0
        and int(structural_pass_count) > 0
        and int(valid_metric_candidates) > 0
    ):
        pool = []
        for d in out:
            cid = str(d.get("candidate_id", ""))
            rank = d.get("composite_rank")
            if not bool(d.get("structural_pass", False)):
                continue
            if cid in stage_killers and bool(stage_killers[cid].get("tail_kill_triggered", False)):
                continue
            if rank is None:
                continue
            try:
                rv = float(rank)
            except Exception:
                continue
            if not math.isfinite(rv):
                continue
            pool.append((cid, rv))
        pool.sort(key=lambda t: (-float(t[1]), str(t[0])))
        if pool:
            keep_id = str(pool[0][0])
            used = True
            selected = [keep_id]
            for d in out:
                cid = str(d.get("candidate_id", ""))
                if cid == keep_id:
                    d["dynamic_pass"] = True
                    d["final_pass"] = True
                    d["fallback_selected"] = True
                    rr = [str(x) for x in (d.get("reasons") or [])]
                    rr.append("stage_salvage_selected")
                    d["reasons"] = sorted(dict.fromkeys(rr))
                else:
                    d["final_pass"] = False

    if bool(dynamic_enabled) and final_pass_count == 0 and int(tail_kill_count) > 0 and not used:
        hard_fail_reason = "tail_kill_switch_dominant_empty_selection"

    return {
        "decisions": out,
        "salvage_used": bool(used),
        "salvage_selected": selected,
        "tail_kill_count": int(tail_kill_count),
        "hard_fail_reason": hard_fail_reason,
    }


def _resolve_universe_limit(args_limit: int, cfg: Dict[str, Any]) -> int:
    ucfg = cfg.get("universe", {}) if isinstance(cfg.get("universe"), dict) else {}
    cfg_limit = int(ucfg.get("limit", 0) or 0)
    effective = int(args_limit) if int(args_limit) > 0 else cfg_limit
    if effective <= 0:
        raise SystemExit("universe_limit_zero_invalid")
    return effective


def _parquet_num_rows(path: str) -> int:
    try:
        import pyarrow.parquet as pq  # type: ignore

        return int(pq.ParquetFile(str(path)).metadata.num_rows)
    except Exception:
        return 0


def _quick_global_precheck(parquet_1d_path: str, policy: GlobalGatePolicy) -> bool:
    try:
        p = Path(parquet_1d_path)
        if not p.exists():
            return False
        df = pd.read_parquet(p, columns=["timestamp", "close"])  # type: ignore[name-defined]
        close = pd.to_numeric(df.get("close"), errors="coerce").dropna()
        if len(close) < int(policy.min_history_days):
            return False
        mdd = float((close / close.cummax() - 1.0).min())
        if not (mdd >= -float(policy.max_drawdown_max)):
            return False
        rets = close.pct_change().dropna()
        vol_ann = float(rets.std(ddof=0) * (252.0 ** 0.5)) if len(rets) else float("nan")
        if vol_ann != vol_ann or vol_ann > float(policy.max_vol_annual):
            return False
        return True
    except Exception:
        return False


def _walkforward_required_bars_from_cfg(train_cfg_path: str) -> int:
    raw = _load_yaml(train_cfg_path)
    splits = raw.get("splits", {}) if isinstance(raw.get("splits"), dict) else {}
    min_train = int(splits.get("min_train_size", 500))
    min_test = int(splits.get("min_test_size", 100))
    fallback_min_train = max(100, max(1, min_train // 2))
    fallback_min_test = max(30, max(1, min_test // 2))
    return int(fallback_min_train + fallback_min_test)


def _resolve_runtime_timeframes(raw_cfg: Dict[str, Any]) -> Dict[str, Any]:
    default_tfs = ["1D", "1H", "30M", "5M", "1M"]
    base_allowed = set(default_tfs)
    allow_ext_raw = raw_cfg.get("allowed_timeframes")
    allow_ext = []
    if isinstance(allow_ext_raw, list):
        allow_ext = [normalize_timeframe(str(t)) for t in allow_ext_raw if str(t).strip()]
    allowed = set(base_allowed)
    allowed.update(allow_ext)

    tfs_raw = raw_cfg.get("timeframes") or default_tfs
    cascade_raw = raw_cfg.get("cascade_order") or default_tfs
    tfs = [normalize_timeframe(str(t)) for t in tfs_raw]
    cascade_order = [normalize_timeframe(str(t)) for t in cascade_raw]
    tfs = list(dict.fromkeys(tfs))
    cascade_order = list(dict.fromkeys(cascade_order))

    for tf in tfs + cascade_order:
        if tf not in allowed:
            return {
                "ok": False,
                "reason": f"config_invalid:unexpected_timeframe:{tf}",
                "timeframes": tfs,
                "cascade_order": cascade_order,
                "allowed_timeframes": sorted(allowed),
                "unexpected_timeframe": tf,
            }

    extra_runtime = sorted(set(cascade_order) - set(tfs))
    extra_quality = sorted(set(tfs) - set(cascade_order))
    if extra_runtime or extra_quality:
        tf = str((extra_runtime or extra_quality)[0])
        return {
            "ok": False,
            "reason": f"config_invalid:unexpected_timeframe:{tf}",
            "timeframes": tfs,
            "cascade_order": cascade_order,
            "allowed_timeframes": sorted(allowed),
            "unexpected_timeframe": tf,
            "extra_in_cascade_order": extra_runtime,
            "extra_in_timeframes": extra_quality,
        }

    return {
        "ok": True,
        "reason": None,
        "timeframes": tfs,
        "cascade_order": cascade_order,
        "allowed_timeframes": sorted(allowed),
        "unexpected_timeframe": None,
    }


def _run_lifecycle_promotion_step(
    run_ctx: Dict[str, Any],
    run_dir: Path,
    cfg: Dict[str, Any],
    policy_path: str,
) -> None:
    """Run the I6 lifecycle promotion evaluation (research context only).

    Reads registry.jsonl and promotion_candidates (if present), evaluates
    lifecycle transitions deterministically, and appends promotion_event
    entries.  Any exception is caught and written to an error JSON; the
    autopilot is never interrupted.
    """
    if str(run_ctx.get("stage", "")).strip().lower() != "research":
        return
    try:
        from octa.core.governance.lifecycle_controller import (
            LifecycleController,
            LifecycleControllerConfig,
        )

        policy_raw: Dict[str, Any] = {}
        try:
            policy_raw = _load_yaml(policy_path)
        except Exception:
            pass
        live_arming = policy_raw.get("live_arming") if isinstance(policy_raw.get("live_arming"), dict) else {}
        safety = policy_raw.get("safety") if isinstance(policy_raw.get("safety"), dict) else {}
        paper_cfg = cfg.get("paper") if isinstance(cfg.get("paper"), dict) else {}

        candidates_path = None
        paper_eval_out = str((paper_cfg or {}).get("eval_out_dir", "")).strip()
        if paper_eval_out:
            _cand = Path(paper_eval_out) / "promotion_candidates.json"
            if _cand.exists():
                candidates_path = _cand

        lc_cfg = LifecycleControllerConfig(
            registry_path=Path("octa") / "var" / "registry" / "models" / "registry.jsonl",
            candidates_path=candidates_path,
            token_path=Path(str(live_arming.get("token_path", "octa/var/state/live_armed.json"))),
            ttl_seconds=int(live_arming.get("ttl_seconds", 900)),
            require_blessed_1d_1h=bool(safety.get("require_blessed_1d_1h", True)),
        )
        lc = LifecycleController(lc_cfg)
        decisions = lc.run(run_ctx, evidence_dir=run_dir / "lifecycle_promotion")

        summary: Dict[str, Any] = {
            "total": len(decisions),
            "allowed": sum(1 for d in decisions if d.allowed),
            "by_to_status": {},
        }
        for d in decisions:
            summary["by_to_status"][d.to_status] = summary["by_to_status"].get(d.to_status, 0) + 1
        _write_json(run_dir / "lifecycle_promotion_summary.json", summary)
    except Exception as _lc_err:
        _write_json(
            run_dir / "lifecycle_promotion_error.json",
            {"error": str(_lc_err), "type": type(_lc_err).__name__},
        )


def main() -> None:
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--version", action="store_true", default=False)
    pre.add_argument("--about", action="store_true", default=False)
    pre.add_argument("--no-banner", action="store_true", default=False)
    pre_args, _ = pre.parse_known_args()
    if pre_args.version:
        print(PLATFORM_NAME)
        return
    if pre_args.about:
        print(f"{BRAND_NAME} | {TAGLINE}")
        return

    ap = argparse.ArgumentParser(
        description="OCTA Autopilot: universe→gates→cascade training→promote to paper"
    )
    ap.add_argument("--config", required=True, help="Autopilot config YAML")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument(
        "--run-paper",
        action="store_true",
        help="After promotion, attempt to run paper_runner (fail-closed if broker not wired)",
    )
    ap.add_argument("--version", action="store_true", default=False)
    ap.add_argument("--about", action="store_true", default=False)
    ap.add_argument("--no-banner", action="store_true", default=False)
    args = ap.parse_args()

    print_banner_once(enabled=not args.no_banner)

    cfg = _load_yaml(args.config)
    debug_cfg = cfg.get("debug", {}) if isinstance(cfg.get("debug"), dict) else {}
    debug_one_symbol = str(debug_cfg.get("one_symbol", "") or "").strip()
    debug_max_internal_steps = int(debug_cfg.get("max_internal_steps", 0) or 0)
    run_id = (
        args.run_id
        or cfg.get("run_id")
        or now_utc_iso().replace(":", "").replace("-", "")[:15] + "Z"
    )

    paper_cfg = cfg.get("paper") if isinstance(cfg.get("paper"), dict) else {}
    requested_mode = str(cfg.get("mode", "") or "").strip().lower()
    inferred_mode = (
        requested_mode
        if requested_mode in {"shadow", "paper", "live"}
        else ("live" if bool(paper_cfg.get("live_enable", False)) else "paper")
    )
    execution_active = bool(args.run_paper) or bool(paper_cfg.get("enabled", False))
    policy_path = str(cfg.get("policy_path", "configs/policy.yaml"))
    policy_flags = _policy_execution_flags(policy_path) if execution_active else {}
    run_ctx: Dict[str, Any] = {
        "mode": inferred_mode,
        "stage": "research",
        "service": "autopilot",
        "execution_active": bool(execution_active),
        "run_id": str(run_id),
        "entrypoint": "execution_service" if execution_active else "autopilot",
        "policy_flags": policy_flags,
    }

    budgets = cfg.get("budgets", {}) if isinstance(cfg.get("budgets"), dict) else {}
    budget = ResourceBudgetController(
        max_runtime_s=int(budgets.get("max_runtime_s", 3600)),
        max_ram_mb=int(budgets.get("max_ram_mb", 12000)),
        max_threads=int(budgets.get("max_threads", 4)),
        max_disk_mb=int(budgets.get("max_disk_mb", 0) or 0),
        disk_root=str(Path("artifacts") / "runs" / run_id),
    )
    budget.apply_thread_caps()

    reg = ArtifactRegistry(root=str(cfg.get("registry_root", "artifacts")), ctx=run_ctx)
    reg.record_run_start(run_id, cfg)

    run_dir = Path("artifacts") / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    model_registry_deps_fingerprint: str | None = None
    tf_resolved = _resolve_runtime_timeframes(cfg)
    _write_resolved_config_snapshot(
        run_dir,
        {
            "timeframe_resolution": {
                "timeframes": list(tf_resolved.get("timeframes", [])),
                "cascade_order": list(tf_resolved.get("cascade_order", [])),
                "allowed_timeframes": list(tf_resolved.get("allowed_timeframes", [])),
                "unexpected_timeframe": tf_resolved.get("unexpected_timeframe"),
                "ok": bool(tf_resolved.get("ok", False)),
            }
        },
    )
    if not bool(tf_resolved.get("ok", False)):
        reason = str(tf_resolved.get("reason") or "config_invalid:unexpected_timeframe")
        reg.record_run_end(run_id, "FAIL", note=reason)
        raise SystemExit(reason)
    tfs = [str(t) for t in tf_resolved.get("timeframes", [])]
    cascade_order = [str(t) for t in tf_resolved.get("cascade_order", [])]
    runtime_profile = str(cfg.get("runtime_profile", "default") or "default").strip().lower()
    if runtime_profile not in {"default", "fast_smoke", "smoke_plus"}:
        reason = f"config_invalid:runtime_profile:{runtime_profile}"
        reg.record_run_end(run_id, "FAIL", note=reason)
        raise SystemExit(reason)
    _write_resolved_config_snapshot(run_dir, {"runtime_profile": runtime_profile})
    dynamic_gate_cfg, dynamic_gate_snapshot = _load_dynamic_gate_cfg(cfg)
    dynamic_gate_status: Dict[str, Any] = {
        "enabled": bool(dynamic_gate_cfg.enabled),
        "configured": bool(dynamic_gate_snapshot.get("configured")),
        "status": "dynamic_gate_not_configured"
        if not bool(dynamic_gate_snapshot.get("configured"))
        else ("dynamic_gate_enabled" if bool(dynamic_gate_cfg.enabled) else "dynamic_gate_disabled"),
        "method": str(dynamic_gate_cfg.method),
        "fallback_used": False,
        "hard_fail_reason": None,
        "population_n_by_stage": {},
        "stage_results": {},
    }
    _write_dynamic_gate_evidence(run_dir, dynamic_gate_snapshot, dynamic_gate_status)
    validation_errors = [str(x) for x in (dynamic_gate_snapshot.get("validation_errors") or [])]
    if bool(dynamic_gate_cfg.enabled) and validation_errors:
        reason = f"dynamic_gate_invalid_config:{';'.join(validation_errors)}"
        dynamic_gate_status["hard_fail_reason"] = reason
        _write_dynamic_gate_evidence(run_dir, dynamic_gate_snapshot, dynamic_gate_status)
        reg.record_run_end(run_id, "FAIL", note=reason)
        raise SystemExit(reason)

    # Global gate (1D only)
    gg_policy = GlobalGatePolicy(**(cfg.get("global_gate", {}) or {}))

    # Universe discovery
    effective_universe_limit = _resolve_universe_limit(int(args.limit), cfg)
    _write_json(
        run_dir / "universe_status.json",
        {"limit": int(effective_universe_limit), "discovered_symbols": 0, "status": "pending_discovery"},
    )
    ucfg = cfg.get("universe", {}) if isinstance(cfg.get("universe"), dict) else {}
    universe = discover_universe(
        stock_dir=str(ucfg.get("stock_dir", "raw/Stock_parquet")),
        fx_dir=str(ucfg.get("fx_dir", "raw/FX_parquet")),
        crypto_dir=str(ucfg.get("crypto_dir", "raw/Crypto_parquet")),
        futures_dir=str(ucfg.get("futures_dir", "raw/Futures_Parquet")),
        etf_dir=str(ucfg.get("etf_dir", "raw/ETF_Parquet")),
        index_dir=str(ucfg.get("index_dir", "raw/Indices_parquet")),
        asset_map_path=str(ucfg.get("asset_map_path", "assets/asset_map.yaml")),
        limit=0,
    )
    total_discovered = int(len(universe))
    ranked_universe = sorted(
        universe,
        key=lambda u: (
            -int(_parquet_num_rows(str((u.parquet_paths or {}).get("1D") or ""))),
            str(u.symbol),
        ),
    )
    scan_n = max(int(effective_universe_limit) * 20, 100)
    preferred: list[Any] = []
    preferred_ids: set[str] = set()
    for u in ranked_universe[:scan_n]:
        p1d = str((u.parquet_paths or {}).get("1D") or "")
        if not p1d:
            continue
        if _quick_global_precheck(p1d, gg_policy):
            preferred.append(u)
            preferred_ids.add(str(u.symbol))
            if len(preferred) >= int(effective_universe_limit):
                break
    selected = list(preferred)
    if len(selected) < int(effective_universe_limit):
        for u in ranked_universe:
            if str(u.symbol) in preferred_ids:
                continue
            selected.append(u)
            if len(selected) >= int(effective_universe_limit):
                break
    universe = selected[: int(effective_universe_limit)]
    if debug_one_symbol:
        universe = [u for u in universe if str(u.symbol) == str(debug_one_symbol)]
        if not universe:
            raise SystemExit(f"debug_one_symbol_not_in_selected_universe:{debug_one_symbol}")
    _write_json(
        run_dir / "universe_status.json",
        {
            "limit": int(effective_universe_limit),
            "discovered_symbols": int(len(universe)),
            "total_discovered_symbols": total_discovered,
            "selection_method": "top_1d_history_rows_then_symbol",
            "debug_one_symbol": str(debug_one_symbol) if debug_one_symbol else None,
            "status": "discovery_complete",
        },
    )

    (run_dir / "universe_candidates.json").write_text(
        json.dumps([u.__dict__ for u in universe], ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    # Data quality gate
    dq_policy = DataQualityPolicy(**(cfg.get("data_quality", {}) or {}))
    dq_decisions = []
    for u in universe:
        budget.checkpoint("dq")
        for tf in tfs:
            pq = (u.parquet_paths or {}).get(tf)
            if not pq:
                dq_decisions.append(
                    GateDecision(
                        u.symbol, tf, "data_quality", "SKIP", "missing_parquet", {"expected": tf}
                    )
                )
                continue
            dq_decisions.append(
                evaluate_data_quality(
                    symbol=u.symbol,
                    timeframe=tf,
                    parquet_path=pq,
                    asset_class=u.asset_class,
                    policy=dq_policy,
                )
            )
            reg.upsert_gate(
                run_id,
                u.symbol,
                tf,
                "data_quality",
                dq_decisions[-1].status,
                dq_decisions[-1].reason,
                json.dumps(dq_decisions[-1].details or {}, default=str),
            )

    write_quality_outputs(run_dir=str(run_dir), decisions=dq_decisions, timeframes=tfs)

    global_decisions = {}
    for u in universe:
        budget.checkpoint("global")
        pq1d = (u.parquet_paths or {}).get("1D")
        if not pq1d:
            global_decisions[u.symbol] = GateDecision(
                u.symbol, "1D", "global", "SKIP", "missing_1d", {}
            )
            continue
        d = evaluate_global_gate(
            symbol=u.symbol,
            parquet_1d_path=pq1d,
            policy=gg_policy,
            cache_dir=str(run_dir / "global_features_store"),
        )
        global_decisions[u.symbol] = d
        reg.upsert_gate(
            run_id,
            u.symbol,
            "1D",
            "global",
            d.status,
            d.reason,
            json.dumps(d.details or {}, default=str),
        )

    write_global_outputs(run_dir=str(run_dir), decisions=global_decisions)
    dq_by_symbol_tf = {
        (str(d.symbol), normalize_timeframe(d.timeframe)): d
        for d in dq_decisions
    }

    # Cascaded training
    if is_production_context(run_ctx):
        assert_write_allowed(
            run_ctx,
            operation="training_tick",
            target="autopilot_cascade_training",
            details={"run_id": str(run_id), "execution_active": bool(execution_active)},
        )
    train_cfg_path = str(cfg.get("training_config", "configs/dev.yaml"))
    _write_resolved_config_snapshot(run_dir, {"training_config_path": train_cfg_path})

    train_decisions = []
    dynamic_thresholds: Dict[str, Any] = {}
    population_stats: Dict[str, Any] = {}
    reason_counts_after: Dict[str, Dict[str, int]] = {}
    decision_rows_csv: list[Dict[str, Any]] = []
    killer_autopsy: Dict[str, Dict[str, Any]] = {}
    killer_reason_counts: Dict[str, Dict[str, int]] = {}
    structural_rows: list[Dict[str, Any]] = []
    structural_reason_counts: Dict[str, Dict[str, int]] = {}
    stage_filter_report: Dict[str, Any] = {}
    stage_symbols_sample: Dict[str, Any] = {}
    step_budgets = _stage_timeouts(cfg)
    training_budget_cfg = _load_training_budget_cfg(cfg, step_budgets, runtime_profile=runtime_profile)
    if bool(training_budget_cfg.get("enabled", False)):
        step_budgets["training_symbol"] = int(training_budget_cfg.get("per_symbol_timeout_s", step_budgets["training_symbol"]))
        step_budgets["training_loop"] = int(training_budget_cfg.get("stage_timeout_s", step_budgets["training_loop"]))
    symbol_by_id = {str(u.symbol): u for u in universe}
    universe_rank = {str(u.symbol): idx for idx, u in enumerate(universe)}
    reports_dir = Path(cfg.get("reports_dir", "reports"))
    training_budget_status: Dict[str, Any] = {
        "runtime_profile": str(runtime_profile),
        "enabled": bool(training_budget_cfg.get("enabled", False)),
        "max_train_symbols_per_tf": training_budget_cfg.get("max_train_symbols_per_tf", 0),
        "resolved_max_train_symbols_per_tf": {},
        "selection_method": str(training_budget_cfg.get("selection_method", "history_ranked")),
        "decouple_tf_pool_from_prev_tf": bool(training_budget_cfg.get("decouple_tf_pool_from_prev_tf", True)),
        "per_symbol_timeout_s": int(step_budgets.get("training_symbol", 0)),
        "stage_timeout_s": int(step_budgets.get("training_loop", 0)),
        "stop_after_first_candidate": bool(training_budget_cfg.get("stop_after_first_candidate", False)),
        "selected_symbols_per_tf": {},
        "skipped_symbols_per_tf": {},
        "timeouts": [],
        "reasons": [],
    }
    _write_json(run_dir / "training_budget_status.json", training_budget_status)

    eligible_symbols: list[str] = []
    for u in sorted(universe, key=lambda x: str(x.symbol)):
        dq1d = next(
            (
                d
                for d in dq_decisions
                if d.symbol == u.symbol and normalize_timeframe(d.timeframe) == "1D"
            ),
            None,
        )
        gg = global_decisions.get(u.symbol)
        if dq1d and dq1d.status == "PASS" and gg and gg.status == "PASS":
            eligible_symbols.append(str(u.symbol))

    # Always emit deterministic precheck projections for all cascade timeframes.
    # This ensures tf visibility in artifacts even when earlier heavy stages timeout.
    for tf0 in cascade_order:
        dq_pass0 = [s for s in sorted(symbol_by_id.keys()) if (dq_by_symbol_tf.get((s, tf0)) and dq_by_symbol_tf[(s, tf0)].status == "PASS")]
        if tf0 == "1D":
            gg_pass0 = [s for s in dq_pass0 if (global_decisions.get(s) and global_decisions[s].status == "PASS")]
        else:
            gg_pass0 = list(dq_pass0)
        stage_filter_report[str(tf0)] = {
            "discovered": int(len(symbol_by_id)),
            "after_data_quality": int(len(dq_pass0)),
            "after_global_gate": int(len(gg_pass0)),
            "projection_only": True,
        }
        stage_symbols_sample[str(tf0)] = {
            "discovered": _first20_symbols(list(symbol_by_id.keys())),
            "after_data_quality": _first20_symbols(dq_pass0),
            "after_global_gate": _first20_symbols(gg_pass0),
            "projection_only": True,
        }
    _write_stage_filter_state(run_dir, stage_filter_report=stage_filter_report, stage_symbols_sample=stage_symbols_sample)

    for tf in cascade_order:
        tf_timer = time.monotonic()
        stage_filter_report[str(tf)] = {}
        stage_symbols_sample[str(tf)] = {}
        stage_state = _stage_state_init(tf=str(tf), discovered=int(len(symbol_by_id)))
        _write_stage_state(run_dir, tf=str(tf), state=stage_state)
        _append_stage_progress(run_dir, tf=tf, step="stage", event="start", elapsed_s=0.0, counts={"discovered": len(symbol_by_id)})
        stage_candidates: list[Dict[str, Any]] = []
        stage_by_symbol: Dict[str, Dict[str, Any]] = {}
        if tf not in dynamic_thresholds:
            dynamic_thresholds[tf] = {
                "enabled": bool(dynamic_gate_cfg.enabled),
                "method": str(dynamic_gate_cfg.method),
                "hard_fail": False,
                "hard_fail_reason": None,
                "fallback_used": False,
                "salvage_used": False,
                "selected": [],
                "note": "stage_pending",
            }
        if tf not in population_stats:
            population_stats[tf] = {"total_candidates": 0, "structural_pass_count": 0, "valid_metric_candidates": 0}
        if tf not in reason_counts_after:
            reason_counts_after[tf] = {}
        if tf not in killer_autopsy:
            killer_autopsy[tf] = {}
        if tf not in killer_reason_counts:
            killer_reason_counts[tf] = {
                "tail_kill_switch": 0,
                "is_oos_degradation": 0,
                "stage_candidates": 0,
            }
        _write_training_selection_artifacts(run_dir, dynamic_thresholds, population_stats, reason_counts_after, decision_rows_csv)
        _write_json(run_dir / "killer_autopsy.json", killer_autopsy)
        _write_json(run_dir / "killer_reason_counts.json", killer_reason_counts)
        dq_pass_symbols = [s for s in sorted(symbol_by_id.keys()) if (dq_by_symbol_tf.get((s, tf)) and dq_by_symbol_tf[(s, tf)].status == "PASS")]
        if tf == "1D":
            gg_pass_symbols = [s for s in dq_pass_symbols if (global_decisions.get(s) and global_decisions[s].status == "PASS")]
        else:
            gg_pass_symbols = list(dq_pass_symbols)
        pool_source, eligible_set = _resolve_stage_pool(
            tf=str(tf),
            eligible_symbols=list(eligible_symbols),
            gg_pass_symbols=list(gg_pass_symbols),
            decouple_tf_pool_from_prev_tf=bool(training_budget_cfg.get("decouple_tf_pool_from_prev_tf", True)),
        )
        stage_state["pool_source"] = str(pool_source)
        stage_filter_report[str(tf)].update(
            {
                "discovered": int(len(symbol_by_id)),
                "after_data_quality": int(len(dq_pass_symbols)),
                "after_global_gate": int(len(gg_pass_symbols)),
            }
        )
        stage_state["after_data_quality"] = int(len(dq_pass_symbols))
        stage_state["after_global_gate"] = int(len(gg_pass_symbols))
        stage_symbols_sample[str(tf)].update(
            {
                "discovered": _first20_symbols(list(symbol_by_id.keys())),
                "after_data_quality": _first20_symbols(dq_pass_symbols),
                "after_global_gate": _first20_symbols(gg_pass_symbols),
            }
        )
        _write_stage_filter_state(run_dir, stage_filter_report=stage_filter_report, stage_symbols_sample=stage_symbols_sample)
        _write_stage_state(run_dir, tf=str(tf), state=stage_state)
        _append_stage_progress(
            run_dir,
            tf=tf,
            step="prepare_filters",
            event="end",
            elapsed_s=float(time.monotonic() - tf_timer),
            counts={
                "discovered": int(len(symbol_by_id)),
                "after_data_quality": int(len(dq_pass_symbols)),
                "after_global_gate": int(len(gg_pass_symbols)),
            },
        )
        if int(time.monotonic() - tf_timer) > int(step_budgets.get("prepare_filters", 600)):
            _append_stage_progress(run_dir, tf=tf, step="prepare_filters", event="timeout", elapsed_s=float(time.monotonic() - tf_timer), counts=stage_filter_report[str(tf)])
            stage_state["status"] = "hard_fail"
            stage_state["reason"] = "stage_step_timeout:prepare_filters"
            _write_stage_state(run_dir, tf=str(tf), state=stage_state)
            raise SystemExit("stage_step_timeout:prepare_filters")
        if int(len(gg_pass_symbols)) == 0:
            _append_stage_progress(run_dir, tf=tf, step="prepare_filters", event="empty", elapsed_s=float(time.monotonic() - tf_timer), counts=stage_filter_report[str(tf)])
            stage_state["top_fail_reasons"] = ["no_symbols_after_precheck"]
            _fail_stage_empty(
                tf=str(tf),
                step="after_global_gate",
                reason="no_symbols_after_precheck",
                run_dir=run_dir,
                stage_state=stage_state,
                stage_filter_report=stage_filter_report,
                stage_symbols_sample=stage_symbols_sample,
                precheck_audit={
                    "symbols": sorted(symbol_by_id.keys()),
                    "passed_symbols": gg_pass_symbols,
                    "dq_by_symbol_tf": dq_by_symbol_tf,
                    "global_decisions": global_decisions,
                },
            )
        required_bars = _walkforward_required_bars_from_cfg(train_cfg_path)
        structural_timer = time.monotonic()
        stage_structural_rows_pre: list[Dict[str, Any]] = []
        stage_reason_counter_pre: Counter[str] = Counter()
        for symbol in sorted(symbol_by_id.keys()):
            dq_dec = dq_by_symbol_tf.get((symbol, tf))
            gg_dec = global_decisions.get(symbol) if tf == "1D" else None
            rule_ids: list[str] = []
            vals: Dict[str, Any] = {}
            spass = False
            note = "excluded_before_structural_training"

            if symbol not in eligible_set:
                if tf == "1D":
                    if dq_dec is None or dq_dec.status != "PASS":
                        rule_ids.append(f"pre_gate_data_quality:{(dq_dec.reason if dq_dec else 'missing')}")
                        vals["data_quality"] = dq_dec.details if dq_dec and isinstance(dq_dec.details, dict) else {}
                    if gg_dec is None or gg_dec.status != "PASS":
                        rule_ids.append(f"pre_gate_global:{(gg_dec.reason if gg_dec else 'missing')}")
                        vals["global_gate"] = gg_dec.details if gg_dec and isinstance(gg_dec.details, dict) else {}
                else:
                    rule_ids.append("pre_gate_not_eligible_for_stage")
            else:
                u0 = symbol_by_id.get(symbol)
                pq = str(((u0.parquet_paths or {}).get(tf)) if u0 is not None else "")
                available_bars = _parquet_num_rows(pq) if pq else 0
                vals["walkforward_precheck"] = {
                    "available_bars": int(available_bars),
                    "required_bars": int(required_bars),
                }
                if not pq:
                    rule_ids.append("missing_parquet")
                elif int(available_bars) < int(required_bars):
                    rule_ids.append("insufficient_history_for_walkforward")
                else:
                    spass = True
                    note = "structural_precheck_pass"

            rule_ids = sorted(dict.fromkeys(rule_ids))
            row = {
                "timeframe": tf,
                "symbol_id": symbol,
                "structural_pass": bool(spass),
                "failing_rule_ids": rule_ids,
                "failing_rule_values": vals,
                "notes": note,
            }
            stage_structural_rows_pre.append(row)
            for rid in rule_ids:
                stage_reason_counter_pre[str(rid)] += 1

        structural_rows.extend(stage_structural_rows_pre)
        structural_reason_counts[tf] = {k: int(v) for k, v in sorted(stage_reason_counter_pre.items(), key=lambda kv: kv[0])}
        _write_structural_audit_artifacts(run_dir, structural_rows, structural_reason_counts)
        structural_pass_count_tf = sum(1 for r in stage_structural_rows_pre if bool(r.get("structural_pass", False)))
        structural_pass_symbols_tf = [str(r.get("symbol_id")) for r in stage_structural_rows_pre if bool(r.get("structural_pass", False))]
        stage_filter_report[str(tf)]["after_structural"] = int(structural_pass_count_tf)
        stage_filter_report[str(tf)]["after_pretrain_precheck"] = int(structural_pass_count_tf)
        stage_state["structural_pass"] = int(structural_pass_count_tf)
        stage_state["after_pretrain_precheck"] = int(structural_pass_count_tf)
        stage_symbols_sample[str(tf)]["after_structural"] = _first20_symbols(structural_pass_symbols_tf)
        stage_symbols_sample[str(tf)]["after_pretrain_precheck"] = _first20_symbols(structural_pass_symbols_tf)
        _write_stage_filter_state(run_dir, stage_filter_report=stage_filter_report, stage_symbols_sample=stage_symbols_sample)
        _write_stage_state(run_dir, tf=str(tf), state=stage_state)
        _append_stage_progress(
            run_dir,
            tf=tf,
            step="structural_precheck",
            event="end",
            elapsed_s=float(time.monotonic() - structural_timer),
            counts={"after_structural": int(structural_pass_count_tf)},
        )
        if int(time.monotonic() - structural_timer) > int(step_budgets.get("structural_precheck", 600)):
            _append_stage_progress(run_dir, tf=tf, step="structural_precheck", event="timeout", elapsed_s=float(time.monotonic() - structural_timer), counts=stage_filter_report[str(tf)])
            stage_state["status"] = "hard_fail"
            stage_state["reason"] = "stage_step_timeout:structural_precheck"
            _write_stage_state(run_dir, tf=str(tf), state=stage_state)
            raise SystemExit("stage_step_timeout:structural_precheck")
        if structural_pass_count_tf == 0:
            reason = "no_structural_pass_candidates"
            dynamic_thresholds[tf] = {
                "enabled": bool(dynamic_gate_cfg.enabled),
                "method": str(dynamic_gate_cfg.method),
                "hard_fail": True,
                "hard_fail_reason": reason,
                "fallback_used": False,
                "salvage_used": False,
                "selected": [],
                "note": "no_stage_candidates_after_structural_precheck",
            }
            population_stats[tf] = {
                "total_candidates": 0,
                "structural_pass_count": 0,
                "valid_metric_candidates": 0,
            }
            reason_counts_after[tf] = {"structural_reject": int(len(stage_structural_rows_pre))}
            dynamic_gate_status["hard_fail_reason"] = reason
            dynamic_gate_status["stage_results"][tf] = {
                "population_n": 0,
                "valid_metric_candidates": 0,
                "fallback_used": False,
                "hard_fail": True,
                "hard_fail_reason": reason,
            }
            dynamic_gate_status["population_n_by_stage"][tf] = 0
            _write_training_selection_artifacts(run_dir, dynamic_thresholds, population_stats, reason_counts_after, decision_rows_csv)
            _write_dynamic_gate_evidence(run_dir, dynamic_gate_snapshot, dynamic_gate_status)
            reg.record_run_end(run_id, "FAIL", note=reason)
            _append_stage_progress(run_dir, tf=tf, step="structural_precheck", event="empty", elapsed_s=float(time.monotonic() - tf_timer), counts=stage_filter_report[str(tf)])
            stage_state["top_fail_reasons"] = _top_fail_reasons(structural_reason_counts.get(tf, {}))
            _fail_stage_empty(
                tf=str(tf),
                step="after_structural",
                reason="no_structural_pass_candidates",
                run_dir=run_dir,
                stage_state=stage_state,
                stage_filter_report=stage_filter_report,
                stage_symbols_sample=stage_symbols_sample,
                precheck_audit={
                    "symbols": sorted(symbol_by_id.keys()),
                    "passed_symbols": gg_pass_symbols,
                    "dq_by_symbol_tf": dq_by_symbol_tf,
                    "global_decisions": global_decisions,
                },
            )

        training_timer = time.monotonic()
        stage_symbol_list = _debug_symbol_scope(
            structural_pass_symbols_tf,
            one_symbol=str(debug_one_symbol),
            max_internal_steps=int(debug_max_internal_steps),
        )
        stage_state["pool_size_before_budget"] = int(len(stage_symbol_list))
        if bool(training_budget_cfg.get("enabled", False)):
            tf_budget_cap, tf_budget_explicit = _resolve_max_train_cap_for_tf(
                max_train_symbols_per_tf=training_budget_cfg.get("max_train_symbols_per_tf", 0),
                tf=str(tf),
            )
            selected_syms, skipped_syms = _select_training_subset(
                stage_symbol_list,
                max_train_symbols_per_tf=int(tf_budget_cap),
                universe_rank=universe_rank,
                selection_method=str(training_budget_cfg.get("selection_method", "history_ranked")),
            )
            if bool(tf_budget_explicit) and int(tf_budget_cap) <= 0:
                selected_syms, skipped_syms = ([], list(_deterministic_train_order(stage_symbol_list, universe_rank=universe_rank, method=str(training_budget_cfg.get("selection_method", "history_ranked")))))
            training_budget_status["resolved_max_train_symbols_per_tf"][str(tf)] = int(tf_budget_cap)
        else:
            selected_syms, skipped_syms = (list(stage_symbol_list), [])
        stage_symbol_list = list(selected_syms)
        stage_state["selected_for_training"] = int(len(stage_symbol_list))
        stage_state["selected_for_training_list"] = [str(s) for s in sorted(stage_symbol_list)]
        training_budget_status["selected_symbols_per_tf"][str(tf)] = list(stage_symbol_list)
        training_budget_status["skipped_symbols_per_tf"][str(tf)] = list(skipped_syms)
        if skipped_syms:
            training_budget_status["reasons"].append(f"{tf}:skipped_due_to_budget:{len(skipped_syms)}")
        _write_json(run_dir / "training_budget_status.json", training_budget_status)
        _append_stage_progress(
            run_dir,
            tf=tf,
            step="budget_select",
            event="end",
            elapsed_s=float(time.monotonic() - training_timer),
            counts={"selected_symbols": int(len(stage_symbol_list)), "skipped_symbols": int(len(skipped_syms))},
        )
        _append_stage_progress(run_dir, tf=tf, step="training_loop", event="start", elapsed_s=0.0, counts={"input_symbols": int(len(stage_symbol_list))})
        for symbol in stage_symbol_list:
            budget.checkpoint("train")
            u = symbol_by_id.get(symbol)
            if u is None:
                continue
            per_symbol_dir = run_dir / "per_symbol" / str(symbol) / str(tf)
            _write_text(per_symbol_dir / "train_stdout_tail.txt", "not_applicable_no_subprocess\n")
            _write_text(per_symbol_dir / "train_stderr_tail.txt", "not_applicable_no_subprocess\n")
            durations: Dict[str, float] = {}
            symbol_timer = time.monotonic()
            _emit_symbol_step(
                per_symbol_dir,
                tf=tf,
                symbol=str(symbol),
                step="training_symbol",
                event="start",
                elapsed_s=0.0,
                details={"run_id": str(run_id)},
            )
            elapsed_training = int(time.monotonic() - training_timer)
            remaining_step = int(step_budgets.get("training_loop", 1200)) - int(elapsed_training)
            per_symbol_budget = min(int(step_budgets.get("training_symbol", 300)), int(remaining_step))
            if per_symbol_budget <= 0:
                _emit_symbol_step(
                    per_symbol_dir,
                    tf=tf,
                    symbol=str(symbol),
                    step="budget_check",
                    event="timeout",
                    elapsed_s=float(time.monotonic() - symbol_timer),
                    details={"remaining_step_s": int(remaining_step), "training_symbol_budget_s": int(per_symbol_budget)},
                )
                _write_json(
                    per_symbol_dir / "exception.json",
                    {
                        "error": "stage_step_timeout:training_loop",
                        "symbol": str(symbol),
                        "tf": str(tf),
                        "remaining_step_s": int(remaining_step),
                    },
                )
                stage_filter_report[str(tf)]["after_training_setup"] = int(len(stage_by_symbol))
                stage_symbols_sample[str(tf)]["after_training_setup"] = _first20_symbols(list(stage_by_symbol.keys()))
                _write_stage_filter_state(run_dir, stage_filter_report=stage_filter_report, stage_symbols_sample=stage_symbols_sample)
                _write_training_selection_artifacts(run_dir, dynamic_thresholds, population_stats, reason_counts_after, decision_rows_csv)
                _write_json(run_dir / "killer_autopsy.json", killer_autopsy)
                _write_json(run_dir / "killer_reason_counts.json", killer_reason_counts)
                _append_stage_progress(run_dir, tf=tf, step="training_loop", event="timeout", elapsed_s=float(time.monotonic() - training_timer), counts={"after_training_setup": int(len(stage_by_symbol))})
                _write_json(per_symbol_dir / "train_step_durations.json", durations)
                training_budget_status["timeouts"].append({"tf": str(tf), "symbol": str(symbol), "step": "training_loop_budget_exhausted"})
                _write_json(run_dir / "training_budget_status.json", training_budget_status)
                if _should_continue_on_training_timeout(training_budget_cfg):
                    stage_state["status"] = "hard_fail"
                    stage_state["reason"] = "budget_exhausted_no_candidates"
                    _write_stage_state(run_dir, tf=str(tf), state=stage_state)
                    break
                raise SystemExit("stage_step_timeout:training_loop")
            try:
                _emit_symbol_step(
                    per_symbol_dir,
                    tf=tf,
                    symbol=str(symbol),
                    step="run_cascade_training",
                    event="start",
                    elapsed_s=float(time.monotonic() - symbol_timer),
                    details={"timeout_s": int(per_symbol_budget)},
                )
                call_start = time.monotonic()
                dlist, metrics_bundle = _run_cascade_training_with_timeout(
                    timeout_s=int(per_symbol_budget),
                    kwargs={
                        "run_id": run_id,
                        "config_path": train_cfg_path,
                        "symbol": u.symbol,
                        "asset_class": u.asset_class,
                        "parquet_paths": u.parquet_paths or {},
                        "cascade": CascadePolicy(order=[tf]),
                        "safe_mode": True,
                        "reports_dir": str(reports_dir),
                        "trace_dir": str(per_symbol_dir),
                    },
                )
                durations["run_cascade_training"] = float(time.monotonic() - call_start)
                _emit_symbol_step(
                    per_symbol_dir,
                    tf=tf,
                    symbol=str(symbol),
                    step="run_cascade_training",
                    event="end",
                    elapsed_s=float(time.monotonic() - symbol_timer),
                    details={"duration_s": float(durations["run_cascade_training"])},
                )
            except _StepTimeoutError:
                _emit_symbol_step(
                    per_symbol_dir,
                    tf=tf,
                    symbol=str(symbol),
                    step="run_cascade_training",
                    event="timeout",
                    elapsed_s=float(time.monotonic() - symbol_timer),
                    details={"timeout_s": int(per_symbol_budget)},
                )
                _write_json(
                    per_symbol_dir / "exception.json",
                    {
                        "error": "stage_step_timeout:training_symbol",
                        "symbol": str(symbol),
                        "tf": str(tf),
                        "timeout_s": int(per_symbol_budget),
                    },
                )
                _write_json(per_symbol_dir / "train_step_durations.json", durations)
                stage_filter_report[str(tf)]["after_training_setup"] = int(len(stage_by_symbol))
                stage_symbols_sample[str(tf)]["after_training_setup"] = _first20_symbols(list(stage_by_symbol.keys()))
                _write_stage_filter_state(run_dir, stage_filter_report=stage_filter_report, stage_symbols_sample=stage_symbols_sample)
                _write_training_selection_artifacts(run_dir, dynamic_thresholds, population_stats, reason_counts_after, decision_rows_csv)
                _write_json(run_dir / "killer_autopsy.json", killer_autopsy)
                _write_json(run_dir / "killer_reason_counts.json", killer_reason_counts)
                _append_stage_progress(run_dir, tf=tf, step="training_loop", event="timeout", elapsed_s=float(time.monotonic() - training_timer), counts={"symbol": str(symbol), "after_training_setup": int(len(stage_by_symbol))})
                training_budget_status["timeouts"].append({"tf": str(tf), "symbol": str(symbol), "step": "training_symbol_timeout"})
                _write_json(run_dir / "training_budget_status.json", training_budget_status)
                if _should_continue_on_training_timeout(training_budget_cfg):
                    continue
                raise SystemExit("stage_step_timeout:training_symbol")
            except Exception as exc:
                _emit_symbol_step(
                    per_symbol_dir,
                    tf=tf,
                    symbol=str(symbol),
                    step="run_cascade_training",
                    event="error",
                    elapsed_s=float(time.monotonic() - symbol_timer),
                    details={"exc_type": str(type(exc).__name__)},
                )
                _write_json(
                    per_symbol_dir / "exception.json",
                    {
                        "error": "training_symbol_exception",
                        "symbol": str(symbol),
                        "tf": str(tf),
                        "exc_type": str(type(exc).__name__),
                        "exc_text": str(exc),
                        "traceback": traceback.format_exc(),
                    },
                )
                _write_json(per_symbol_dir / "train_step_durations.json", durations)
                raise
            d = (
                dlist[0]
                if dlist
                else GateDecision(
                    u.symbol,
                    tf,
                    "train",
                    "FAIL",
                    "missing_stage_decision",
                    {"structural_pass": False, "performance_pass": False},
                )
            )
            mb = metrics_bundle.get(tf, {}) if isinstance(metrics_bundle, dict) else {}
            _emit_symbol_step(
                per_symbol_dir,
                tf=tf,
                symbol=str(symbol),
                step="candidate_build",
                event="end",
                elapsed_s=float(time.monotonic() - symbol_timer),
                details={"has_metrics_bundle": bool(isinstance(metrics_bundle, dict))},
            )
            details = d.details if isinstance(d.details, dict) else {}
            reasons: list[str] = []
            if d.reason:
                reasons.append(str(d.reason))
            gate_blob = mb.get("gate") if isinstance(mb, dict) else None
            if isinstance(gate_blob, dict) and isinstance(gate_blob.get("reasons"), list):
                reasons.extend([str(r) for r in gate_blob["reasons"] if r is not None])
            stage_candidates.append(
                {
                    "candidate_id": str(symbol),
                    "structural_pass": bool(details.get("structural_pass", False)),
                    "static_pass": bool(details.get("performance_pass", False)),
                    "metrics": mb.get("metrics") if isinstance(mb, dict) and isinstance(mb.get("metrics"), dict) else {},
                    "reasons": reasons,
                }
            )
            stage_by_symbol[symbol] = {"decision": d, "metrics_bundle": mb}
            stage_filter_report[str(tf)]["after_training_setup"] = int(len(stage_by_symbol))
            stage_symbols_sample[str(tf)]["after_training_setup"] = _first20_symbols(list(stage_by_symbol.keys()))
            stage_filter_report[str(tf)]["candidates_built"] = int(len(stage_candidates))
            stage_symbols_sample[str(tf)]["candidates_built"] = _first20_symbols([str(c.get("candidate_id", "")) for c in stage_candidates])
            stage_state["trained_completed"] = int(len(stage_by_symbol))
            stage_state["stage_candidates_built"] = int(len(stage_candidates))
            stage_state["after_training_setup"] = int(len(stage_by_symbol))
            stage_state["candidates_built"] = int(len(stage_candidates))
            _write_stage_filter_state(run_dir, stage_filter_report=stage_filter_report, stage_symbols_sample=stage_symbols_sample)
            _write_stage_state(run_dir, tf=str(tf), state=stage_state)
            _write_json(per_symbol_dir / "train_step_durations.json", durations)
            _emit_symbol_step(
                per_symbol_dir,
                tf=tf,
                symbol=str(symbol),
                step="training_symbol",
                event="end",
                elapsed_s=float(time.monotonic() - symbol_timer),
                details={"status": str(d.status), "reason": str(d.reason) if d.reason else None},
            )
            if bool(training_budget_cfg.get("enabled", False)) and bool(training_budget_cfg.get("stop_after_first_candidate", False)) and len(stage_candidates) >= 1:
                _append_stage_progress(
                    run_dir,
                    tf=tf,
                    step="training_loop",
                    event="budget_stop",
                    elapsed_s=float(time.monotonic() - training_timer),
                    counts={"after_training_setup": int(len(stage_by_symbol)), "candidates_built": int(len(stage_candidates))},
                )
                break
            if int(time.monotonic() - training_timer) > int(step_budgets.get("training_loop", 1200)):
                stage_filter_report[str(tf)]["after_training_setup"] = int(len(stage_by_symbol))
                stage_symbols_sample[str(tf)]["after_training_setup"] = _first20_symbols(list(stage_by_symbol.keys()))
                _write_stage_filter_state(run_dir, stage_filter_report=stage_filter_report, stage_symbols_sample=stage_symbols_sample)
                _write_training_selection_artifacts(run_dir, dynamic_thresholds, population_stats, reason_counts_after, decision_rows_csv)
                _write_json(run_dir / "killer_autopsy.json", killer_autopsy)
                _write_json(run_dir / "killer_reason_counts.json", killer_reason_counts)
                _append_stage_progress(run_dir, tf=tf, step="training_loop", event="timeout", elapsed_s=float(time.monotonic() - training_timer), counts={"after_training_setup": int(len(stage_by_symbol))})
                training_budget_status["timeouts"].append({"tf": str(tf), "symbol": str(symbol), "step": "training_loop_timeout"})
                _write_json(run_dir / "training_budget_status.json", training_budget_status)
                if _should_continue_on_training_timeout(training_budget_cfg):
                    break
                raise SystemExit("stage_step_timeout:training_loop")

        stage_filter_report[str(tf)]["after_training_setup"] = int(len(stage_by_symbol))
        stage_symbols_sample[str(tf)]["after_training_setup"] = _first20_symbols(list(stage_by_symbol.keys()))
        stage_state["trained_completed"] = int(len(stage_by_symbol))
        stage_state["stage_candidates_built"] = int(len(stage_candidates))
        stage_state["after_training_setup"] = int(len(stage_by_symbol))
        stage_state["candidates_built"] = int(len(stage_candidates))
        _write_stage_filter_state(run_dir, stage_filter_report=stage_filter_report, stage_symbols_sample=stage_symbols_sample)
        _write_stage_state(run_dir, tf=str(tf), state=stage_state)
        _append_stage_progress(run_dir, tf=tf, step="training_loop", event="end", elapsed_s=float(time.monotonic() - training_timer), counts={"after_training_setup": int(len(stage_by_symbol))})
        if int(len(stage_by_symbol)) == 0:
            if bool(training_budget_cfg.get("enabled", False)):
                training_budget_status["reasons"].append(f"{tf}:budget_exhausted_no_candidates")
                _write_json(run_dir / "training_budget_status.json", training_budget_status)
                _append_stage_progress(run_dir, tf=tf, step="training_loop", event="empty", elapsed_s=float(time.monotonic() - training_timer), counts=stage_filter_report[str(tf)])
                stage_state["status"] = "hard_fail"
                stage_state["reason"] = "budget_exhausted_no_candidates"
                stage_state["top_fail_reasons"] = ["budget_exhausted_no_candidates"]
                _write_stage_state(run_dir, tf=str(tf), state=stage_state)
                raise SystemExit("budget_exhausted_no_candidates")
            _append_stage_progress(run_dir, tf=tf, step="training_loop", event="empty", elapsed_s=float(time.monotonic() - training_timer), counts=stage_filter_report[str(tf)])
            stage_state["top_fail_reasons"] = ["no_dynamic_gate_input_candidates"]
            _fail_stage_empty(
                tf=str(tf),
                step="after_training_setup",
                reason="no_stage_symbols_built",
                run_dir=run_dir,
                stage_state=stage_state,
                stage_filter_report=stage_filter_report,
                stage_symbols_sample=stage_symbols_sample,
                precheck_audit={
                    "symbols": sorted(symbol_by_id.keys()),
                    "passed_symbols": gg_pass_symbols,
                    "dq_by_symbol_tf": dq_by_symbol_tf,
                    "global_decisions": global_decisions,
                },
            )

        stage_killers: Dict[str, Any] = {}
        tail_kill_count = 0
        oos_kill_count = 0
        for symbol in sorted(symbol_by_id.keys()):
            if symbol in stage_by_symbol:
                base = stage_by_symbol[symbol]
                d: GateDecision = base["decision"]
                mb = base["metrics_bundle"] if isinstance(base["metrics_bundle"], dict) else {}
                gate_blob = mb.get("gate") if isinstance(mb, dict) and isinstance(mb.get("gate"), dict) else {}
                reasons: list[str] = []
                if d.reason:
                    reasons.append(str(d.reason))
                if isinstance(gate_blob, dict) and isinstance(gate_blob.get("reasons"), list):
                    reasons.extend([str(x) for x in gate_blob.get("reasons", []) if x is not None])
                aut = _extract_killer_autopsy(gate_blob if isinstance(gate_blob, dict) else {}, reasons)
                aut["candidate_scope"] = "stage_candidate"
            else:
                aut = {
                    "candidate_scope": "excluded_pre_stage",
                    "tail_kill_triggered": False,
                    "tail_inputs": {},
                    "tail_stats": {},
                    "tail_thresholds": {},
                    "oos_degradation_triggered": False,
                    "is_metrics": {},
                    "oos_metrics": {},
                    "degradation_calc": {},
                    "thresholds": {},
                }
            stage_killers[symbol] = aut
            tail_kill_count += 1 if bool(aut.get("tail_kill_triggered", False)) else 0
            oos_kill_count += 1 if bool(aut.get("oos_degradation_triggered", False)) else 0
        killer_autopsy[tf] = stage_killers
        killer_reason_counts[tf] = {
            "tail_kill_switch": int(tail_kill_count),
            "is_oos_degradation": int(oos_kill_count),
            "stage_candidates": int(len(stage_by_symbol)),
        }
        _write_json(run_dir / "killer_autopsy.json", killer_autopsy)
        _write_json(run_dir / "killer_reason_counts.json", killer_reason_counts)
        _write_oos_degradation_autopsy(
            run_dir,
            tf=str(tf),
            stage_by_symbol=stage_by_symbol,
            stage_killers=stage_killers,
        )

        selection_timer = time.monotonic()
        stage_eval = evaluate_training_selection(candidates=stage_candidates, config=dynamic_gate_cfg)
        decisions_list = [dict(x) for x in (stage_eval.get("decisions", []) if isinstance(stage_eval.get("decisions"), list) else [])]
        valid_metric_candidates = int((stage_eval.get("population_stats") or {}).get("valid_metric_candidates", 0))
        salvage = _apply_stage_salvage(
            decisions=decisions_list,
            stage_killers=stage_killers,
            dynamic_enabled=bool(dynamic_gate_cfg.enabled),
            structural_pass_count=int(structural_pass_count_tf),
            valid_metric_candidates=int(valid_metric_candidates),
        )
        decisions_list = [dict(x) for x in salvage.get("decisions", [])]
        stage_salvage_used = bool(salvage.get("salvage_used", False))
        stage_salvage_selected = [str(x) for x in (salvage.get("salvage_selected") or [])]
        if salvage.get("hard_fail_reason"):
            stage_eval["hard_fail"] = True
            stage_eval["hard_fail_reason"] = str(salvage.get("hard_fail_reason"))
            rc_map = dict(stage_eval.get("reason_counts", {}))
            rc_map["tail_kill_switch"] = int(rc_map.get("tail_kill_switch", 0)) + int(tail_kill_count)
            stage_eval["reason_counts"] = rc_map
        stage_eval["decisions"] = decisions_list
        stage_eval["fallback_used"] = bool(stage_eval.get("fallback_used", False) or stage_salvage_used)
        stage_eval["salvage_used"] = bool(stage_salvage_used)
        stage_eval["salvage_selected"] = list(stage_salvage_selected)
        stage_state["dynamic_gate_input_count"] = int(len(stage_candidates))
        stage_filter_report[str(tf)]["candidates_built"] = int(len(stage_candidates))
        stage_symbols_sample[str(tf)]["candidates_built"] = _first20_symbols([str(c.get("candidate_id", "")) for c in stage_candidates])
        _write_stage_filter_state(run_dir, stage_filter_report=stage_filter_report, stage_symbols_sample=stage_symbols_sample)
        _write_stage_state(run_dir, tf=str(tf), state=stage_state)
        _append_stage_progress(run_dir, tf=tf, step="selection_eval", event="end", elapsed_s=float(time.monotonic() - selection_timer), counts={"candidates_built": int(len(stage_candidates))})
        if int(time.monotonic() - selection_timer) > int(step_budgets.get("selection_eval", 600)):
            _write_training_selection_artifacts(run_dir, dynamic_thresholds, population_stats, reason_counts_after, decision_rows_csv)
            _write_json(run_dir / "killer_autopsy.json", killer_autopsy)
            _write_json(run_dir / "killer_reason_counts.json", killer_reason_counts)
            _append_stage_progress(run_dir, tf=tf, step="selection_eval", event="timeout", elapsed_s=float(time.monotonic() - selection_timer), counts=stage_filter_report[str(tf)])
            stage_state["status"] = "hard_fail"
            stage_state["reason"] = "stage_step_timeout:selection_eval"
            _write_stage_state(run_dir, tf=str(tf), state=stage_state)
            raise SystemExit("stage_step_timeout:selection_eval")
        if int(len(stage_candidates)) == 0:
            _append_stage_progress(run_dir, tf=tf, step="selection_eval", event="empty", elapsed_s=float(time.monotonic() - selection_timer), counts=stage_filter_report[str(tf)])
            _fail_stage_empty(
                tf=str(tf),
                step="candidates_built",
                reason="no_stage_candidates_built",
                run_dir=run_dir,
                stage_state=stage_state,
                stage_filter_report=stage_filter_report,
                stage_symbols_sample=stage_symbols_sample,
                precheck_audit={
                    "symbols": sorted(symbol_by_id.keys()),
                    "passed_symbols": gg_pass_symbols,
                    "dq_by_symbol_tf": dq_by_symbol_tf,
                    "global_decisions": global_decisions,
                },
            )

        stage_pop = stage_eval.get("population_stats", {}) if isinstance(stage_eval.get("population_stats"), dict) else {}
        if bool(stage_eval.get("hard_fail", False)):
            stage_eval["hard_fail_reason"] = _normalize_dynamic_hard_fail_reason(
                current_reason=(stage_eval.get("hard_fail_reason")),
                structural_pass_count=int(structural_pass_count_tf),
                stage_candidates_count=int(len(stage_candidates)),
            )
        stage_fallback_used = bool(stage_eval.get("fallback_used", False))
        dynamic_gate_status["population_n_by_stage"][tf] = int(stage_pop.get("total_candidates", len(stage_candidates)))
        dynamic_gate_status["stage_results"][tf] = {
            "population_n": int(stage_pop.get("total_candidates", len(stage_candidates))),
            "valid_metric_candidates": int(stage_pop.get("valid_metric_candidates", 0)),
            "fallback_used": stage_fallback_used,
            "hard_fail": bool(stage_eval.get("hard_fail", False)),
            "hard_fail_reason": stage_eval.get("hard_fail_reason"),
        }
        dynamic_gate_status["fallback_used"] = bool(dynamic_gate_status["fallback_used"] or stage_fallback_used)
        _write_dynamic_gate_evidence(run_dir, dynamic_gate_snapshot, dynamic_gate_status)
        dynamic_thresholds[tf] = dict(stage_eval.get("thresholds", {}))
        dynamic_thresholds[tf]["hard_fail"] = bool(stage_eval.get("hard_fail", False))
        dynamic_thresholds[tf]["hard_fail_reason"] = stage_eval.get("hard_fail_reason")
        dynamic_thresholds[tf]["fallback_used"] = bool(stage_eval.get("fallback_used", False))
        dynamic_thresholds[tf]["salvage_used"] = bool(stage_eval.get("salvage_used", False))
        dynamic_thresholds[tf]["selected"] = list(stage_eval.get("salvage_selected", []))
        population_stats[tf] = dict(stage_eval.get("population_stats", {}))
        reason_counts_after[tf] = dict(stage_eval.get("reason_counts", {}))
        stage_state["top_fail_reasons"] = _top_fail_reasons(reason_counts_after.get(tf, {}))

        decisions_by_symbol = {str(r.get("candidate_id")): r for r in stage_eval.get("decisions", [])}
        stage_state["dynamic_gate_final_pass_count"] = int(sum(1 for r in decisions_by_symbol.values() if bool(r.get("final_pass", False))))
        stage_decisions_added = 0
        next_symbols: list[str] = []
        for symbol in sorted(stage_by_symbol.keys()):
            base = stage_by_symbol[symbol]
            d: GateDecision = base["decision"]
            mb = base["metrics_bundle"] if isinstance(base["metrics_bundle"], dict) else {}
            tf_norm = normalize_timeframe(d.timeframe)
            dec = decisions_by_symbol.get(symbol, {})
            progression_final_pass = bool(dec.get("final_pass", False))
            dynamic_pass = bool(dec.get("dynamic_pass", False))
            composite_rank = dec.get("composite_rank")
            fallback_selected = bool(dec.get("fallback_selected", False))
            decision_reasons = [str(r) for r in (dec.get("reasons") or [])]

            decision_rows_csv.append(
                {
                    "timeframe": tf_norm,
                    "candidate_id": symbol,
                    "metrics_json": json.dumps((mb.get("metrics") if isinstance(mb, dict) else {}) or {}, sort_keys=True, default=str),
                    "sharpe": ((mb.get("metrics") or {}) if isinstance(mb, dict) else {}).get("sharpe"),
                    "profit_factor": ((mb.get("metrics") or {}) if isinstance(mb, dict) else {}).get("profit_factor"),
                    "calmar": ((mb.get("metrics") or {}) if isinstance(mb, dict) else {}).get("calmar"),
                    "max_drawdown": ((mb.get("metrics") or {}) if isinstance(mb, dict) else {}).get("max_drawdown"),
                    "population_n": int(stage_pop.get("total_candidates", len(stage_candidates))),
                    "structural_pass": bool(dec.get("structural_pass", False)),
                    "static_pass": bool(dec.get("static_pass", False)),
                    "dynamic_pass": dynamic_pass,
                    "final_pass": progression_final_pass,
                    "fallback_selected": fallback_selected,
                    "fallback_used": bool(stage_fallback_used),
                    "salvage_used": bool(stage_salvage_used),
                    "composite_rank": composite_rank,
                    "reject_reason_code": str(dec.get("reject_reason_code", "")),
                    "reasons": "|".join(decision_reasons),
                }
            )
            stage_decisions_added += 1

            if isinstance(mb, dict):
                reg.add_metrics(
                    run_id=run_id,
                    symbol=symbol,
                    timeframe=tf_norm,
                    stage="train",
                    metrics_json=json.dumps(mb.get("metrics"), default=str)
                    if mb.get("metrics") is not None
                    else None,
                    gate_json=json.dumps(mb.get("gate"), default=str)
                    if mb.get("gate") is not None
                    else None,
                )

            pkl_path = ""
            sha_txt = ""
            pack = mb.get("pack") if isinstance(mb, dict) else None
            if isinstance(pack, dict):
                pkl_path = str(pack.get("pkl") or "")
                sha_txt = str(pack.get("pkl_sha") or "")

            effective = d
            if progression_final_pass:
                effective = GateDecision(
                    d.symbol,
                    tf_norm,
                    d.stage,
                    "PASS",
                    None,
                    {
                        **(d.details or {}),
                        "dynamic_gate": {
                            "enabled": bool(dynamic_gate_cfg.enabled),
                            "dynamic_pass": dynamic_pass,
                            "final_pass": progression_final_pass,
                            "fallback_selected": fallback_selected,
                            "composite_rank": composite_rank,
                            "reasons": decision_reasons,
                        },
                    },
                )

            if effective.status == "PASS":
                pkl_ok = (
                    bool(pkl_path)
                    and Path(pkl_path).exists()
                    and Path(pkl_path.replace(".pkl", ".sha256")).exists()
                )
                if not pkl_ok:
                    effective = GateDecision(
                        symbol,
                        tf_norm,
                        d.stage,
                        "FAIL",
                        "packaging_missing_pkl",
                        {"expected_pkl": pkl_path, "progression_final_pass": progression_final_pass},
                    )

            train_decisions.append(effective)
            reg.upsert_gate(
                run_id,
                symbol,
                tf_norm,
                "train",
                effective.status,
                effective.reason,
                json.dumps(effective.details or {}, default=str),
            )

            if effective.status in {"FAIL", "SKIP"}:
                try:
                    reg.clear_promotion(symbol=symbol, timeframe=tf_norm, level="paper")
                except Exception:
                    pass

            if effective.status == "PASS" and pkl_path:
                meta_path = pkl_path.replace(".pkl", ".meta.json")
                meta_json = None
                try:
                    if Path(meta_path).exists():
                        meta_json = Path(meta_path).read_text(encoding="utf-8")
                except Exception:
                    meta_json = None
                art_id = reg.add_artifact(
                    run_id, symbol, tf_norm, "tradeable", pkl_path, sha_txt, 1, meta_json=meta_json
                )
                reg.promote(symbol, tf_norm, art_id, level="paper")
                if str(run_ctx.get("stage", "")).strip().lower() == "research":
                    if model_registry_deps_fingerprint is None:
                        model_registry_deps_fingerprint = compute_deps_fingerprint()
                    pkl_size = 0
                    try:
                        pkl_size = int(Path(pkl_path).stat().st_size)
                    except Exception:
                        pkl_size = 0
                    inputs_hash = stable_hash(
                        {
                            "symbol": str(symbol),
                            "timeframe": str(tf_norm),
                            "artifact_sha256": str(sha_txt),
                            "config_hash": stable_hash(cfg),
                            "gate_status": str(effective.status),
                            "gate_reason": str(effective.reason),
                        }
                    )
                    outputs_hash = stable_hash(
                        {
                            "artifact_id": int(art_id),
                            "promotion_level": "paper",
                            "pkl": str(pkl_path),
                            "pkl_sha": str(sha_txt),
                        }
                    )
                    model_entry = build_registry_entry(
                        symbol=str(symbol),
                        timeframe=str(tf_norm),
                        artifact_path=_relpath_or_abs(str(pkl_path), start="."),
                        artifact_sha256=str(sha_txt),
                        artifact_size_bytes=int(pkl_size),
                        feature_code_hash=stable_hash(
                            {
                                "module": "octa_training.core.packaging",
                                "model_name": str(((mb.get("pack") or {}).get("model_name")) if isinstance(mb.get("pack"), dict) else ""),
                                "features_used": list((mb.get("features_used") or []) if isinstance(mb, dict) else []),
                            }
                        ),
                        config_hash=stable_hash(cfg),
                        stage="research",
                        run_id=str(run_id),
                        evidence_dir=_relpath_or_abs(str(run_dir), start="."),
                        inputs_hash=str(inputs_hash),
                        outputs_hash=str(outputs_hash),
                        gates={
                            "structural": "PASS" if str(effective.status) == "PASS" else "FAIL",
                            "risk": "HOLD",
                            "performance": "PASS" if str(effective.status) == "PASS" else "FAIL",
                            "drift": "HOLD",
                        },
                        promotion_status="PAPER",
                        promotion_reason="autopilot_promotion",
                        deps_fingerprint=str(model_registry_deps_fingerprint),
                        asset_class=str(getattr(u, "asset_class", "")),
                        training_data_hash=None,
                        hyperparam_hash=stable_hash(((mb.get("metrics") or {}) if isinstance(mb, dict) else {})),
                        seed=int(cfg.get("seed", 42)) if isinstance(cfg, dict) else 42,
                    )
                    append_model_registry_entry(
                        run_ctx,
                        model_entry,
                        registry_path=Path("octa") / "var" / "registry" / "models" / "registry.jsonl",
                        evidence_dir=run_dir / "model_registry",
                    )

            if progression_final_pass:
                next_symbols.append(symbol)

        invariant_errors = _check_stage_invariants(state=stage_state, stage_decisions_added=int(stage_decisions_added))
        if invariant_errors:
            _write_training_selection_artifacts(run_dir, dynamic_thresholds, population_stats, reason_counts_after, decision_rows_csv)
            _write_json(run_dir / "killer_autopsy.json", killer_autopsy)
            _write_json(run_dir / "killer_reason_counts.json", killer_reason_counts)
            stage_state["status"] = "hard_fail"
            stage_state["reason"] = f"stage_invariant_violation:{tf}:{'|'.join(invariant_errors)}"
            stage_state["top_fail_reasons"] = _top_fail_reasons(reason_counts_after.get(tf, {}))
            _write_stage_state(run_dir, tf=str(tf), state=stage_state)
            raise SystemExit(f"stage_invariant_violation:{tf}:{'|'.join(invariant_errors)}")

        if bool(stage_eval.get("hard_fail", False)):
            _write_training_selection_artifacts(run_dir, dynamic_thresholds, population_stats, reason_counts_after, decision_rows_csv)
            _write_json(run_dir / "killer_autopsy.json", killer_autopsy)
            _write_json(run_dir / "killer_reason_counts.json", killer_reason_counts)
            dynamic_gate_status["hard_fail_reason"] = f"dynamic_gate_hard_fail:{tf}:{stage_eval.get('hard_fail_reason')}"
            training_budget_status["reasons"].append(f"{tf}:dynamic_gate_hard_fail:{stage_eval.get('hard_fail_reason')}")
            _write_json(run_dir / "training_budget_status.json", training_budget_status)
            _write_dynamic_gate_evidence(run_dir, dynamic_gate_snapshot, dynamic_gate_status)
            reg.record_run_end(run_id, "FAIL", note=f"dynamic_gate_hard_fail:{tf}:{stage_eval.get('hard_fail_reason')}")
            _append_stage_progress(run_dir, tf=tf, step="selection_eval", event="hard_fail", elapsed_s=float(time.monotonic() - tf_timer), counts=stage_filter_report[str(tf)])
            stage_state["status"] = "hard_fail"
            stage_state["reason"] = str(stage_eval.get("hard_fail_reason"))
            stage_state["top_fail_reasons"] = _top_fail_reasons(reason_counts_after.get(tf, {}))
            _write_stage_state(run_dir, tf=str(tf), state=stage_state)
            raise SystemExit(f"dynamic_gate_hard_fail:{tf}:{stage_eval.get('hard_fail_reason')}")

        eligible_symbols = sorted(dict.fromkeys(next_symbols))
        stage_state["status"] = "ok"
        stage_state["reason"] = None
        _write_stage_state(run_dir, tf=str(tf), state=stage_state)
        _append_stage_progress(run_dir, tf=tf, step="stage", event="end", elapsed_s=float(time.monotonic() - tf_timer), counts={"next_symbols": int(len(eligible_symbols))})

    write_gate_matrix(run_dir=str(run_dir), decisions=train_decisions, cascade_order=cascade_order)
    _write_training_selection_artifacts(run_dir, dynamic_thresholds, population_stats, reason_counts_after, decision_rows_csv)
    _write_json(run_dir / "killer_autopsy.json", killer_autopsy)
    _write_json(run_dir / "killer_reason_counts.json", killer_reason_counts)

    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "universe": len(universe),
                "dq": len(dq_decisions),
                "global": len(global_decisions),
                "train": len(train_decisions),
                "dynamic_gate_enabled": bool(dynamic_gate_cfg.enabled),
                "registry_db": str(reg.paths.db_path),
                "rss_peak_mb": budget.rss_peak_mb,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    _write_dynamic_gate_evidence(run_dir, dynamic_gate_snapshot, dynamic_gate_status)
    _write_json(run_dir / "training_budget_status.json", training_budget_status)

    reg.record_run_end(run_id, "OK")

    # I6: Lifecycle promotion evaluation (research context, best-effort).
    _run_lifecycle_promotion_step(run_ctx, run_dir, cfg, policy_path)

    # Optional: run paper trading loop immediately after promotion.
    if bool(args.run_paper) or bool((cfg.get("paper") or {}).get("enabled", False)):
        _ = run_paper(
            run_id=run_id,
            config_path=train_cfg_path,
            registry_root=str(cfg.get("registry_root", "artifacts")),
            ledger_dir=str(paper_cfg.get("ledger_dir", "artifacts/ledger_paper")),
            level=str(paper_cfg.get("level", "paper")),
            live_enable=bool(paper_cfg.get("live_enable", False)),
            last_n_rows=int(paper_cfg.get("last_n_rows", 300)),
            paper_log_path=str(paper_cfg.get("paper_log_path", "artifacts/paper_trade_log.ndjson")),
            max_runtime_s=int((paper_cfg.get("budgets") or {}).get("max_runtime_s", 3600)),
            max_ram_mb=int((paper_cfg.get("budgets") or {}).get("max_ram_mb", 12000)),
            max_threads=int((paper_cfg.get("budgets") or {}).get("max_threads", 4)),
        )


if __name__ == "__main__":
    try:
        main()
    except BudgetExceeded as e:
        raise SystemExit(str(e)) from e
