from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence

import math


@dataclass(frozen=True)
class MetricRule:
    direction: str  # "high" or "low"
    weight: float


@dataclass(frozen=True)
class TrainingSelectionGateConfig:
    enabled: bool = False
    method: str = "percentile_rank"
    min_population: int = 30
    top_percentile: float = 0.70
    fallback_top_k_if_empty: int = 1
    required_metrics: tuple[str, ...] = ("sharpe", "profit_factor", "calmar", "max_drawdown")
    metrics: Mapping[str, MetricRule] | None = None


def _default_metric_rules() -> Dict[str, MetricRule]:
    return {
        "sharpe": MetricRule(direction="high", weight=0.35),
        "profit_factor": MetricRule(direction="high", weight=0.25),
        "calmar": MetricRule(direction="high", weight=0.20),
        "max_drawdown": MetricRule(direction="low", weight=0.20),
    }


def _metric_aliases() -> Dict[str, str]:
    return {
        "pf": "profit_factor",
        "profitfactor": "profit_factor",
        "max_dd": "max_drawdown",
        "maxdd": "max_drawdown",
    }


def _finite_number(value: Any) -> float | None:
    try:
        v = float(value)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return v


def _percentile_by_order(
    ids: Sequence[str],
    values: Mapping[str, float],
    *,
    direction: str,
) -> Dict[str, float]:
    # Deterministic stable tie-breaker by candidate id.
    if str(direction).lower() == "low":
        ordered = sorted(ids, key=lambda i: (float(values[i]), str(i)))
    else:
        ordered = sorted(ids, key=lambda i: (-float(values[i]), str(i)))
    n = len(ordered)
    if n <= 1:
        return {ordered[0]: 1.0} if ordered else {}
    out: Dict[str, float] = {}
    for idx, cid in enumerate(ordered):
        out[cid] = 1.0 - (float(idx) / float(n - 1))
    return out


def evaluate_training_selection(
    *,
    candidates: Sequence[Mapping[str, Any]],
    config: TrainingSelectionGateConfig,
) -> Dict[str, Any]:
    metric_rules = dict(config.metrics or _default_metric_rules())
    aliases = _metric_aliases()
    required_metrics = tuple(aliases.get(str(m), str(m)) for m in (config.required_metrics or tuple(metric_rules.keys())))
    candidate_rows = [dict(c) for c in candidates]
    reason_counts: Dict[str, int] = {}
    decisions: List[Dict[str, Any]] = []
    missing_counts: Dict[str, int] = {m: 0 for m in required_metrics}
    valid_metric_values: Dict[str, List[float]] = {m: [] for m in required_metrics}

    def _count(reason: str) -> None:
        reason_counts[reason] = int(reason_counts.get(reason, 0)) + 1

    if not bool(config.enabled):
        for row in sorted(candidate_rows, key=lambda x: str(x.get("candidate_id", ""))):
            cid = str(row.get("candidate_id", ""))
            structural_pass = bool(row.get("structural_pass", False))
            static_pass = bool(row.get("static_pass", False))
            final_pass = bool(structural_pass and static_pass)
            reasons = list(row.get("reasons") or [])
            reject_reason_code = ""
            if not structural_pass:
                reasons.append("structural_reject")
                _count("structural_reject")
                reject_reason_code = "structural_reject"
            elif not static_pass:
                reasons.append("static_reject")
                _count("static_reject")
                reject_reason_code = "static_reject"
            decisions.append(
                {
                    "candidate_id": cid,
                    "structural_pass": structural_pass,
                    "static_pass": static_pass,
                    "dynamic_pass": static_pass,
                    "final_pass": final_pass,
                    "composite_rank": None,
                    "reasons": reasons,
                    "fallback_selected": False,
                    "reject_reason_code": reject_reason_code,
                }
            )
        return {
            "ok": True,
            "hard_fail": False,
            "hard_fail_reason": None,
            "fallback_used": False,
            "thresholds": {
                "enabled": False,
                "method": "disabled_static_passthrough",
                "top_percentile": float(config.top_percentile),
                "min_population": int(config.min_population),
                "fallback_top_k_if_empty": int(config.fallback_top_k_if_empty),
                "required_metrics": list(required_metrics),
            },
            "population_stats": {
                "total_candidates": len(candidate_rows),
                "structural_pass_count": sum(1 for r in candidate_rows if bool(r.get("structural_pass", False))),
                "valid_metric_candidates": 0,
                "missing_metric_counts": missing_counts,
            },
            "reason_counts": reason_counts,
            "decisions": decisions,
        }

    structural_rows = [
        r for r in candidate_rows if bool(r.get("structural_pass", False))
    ]
    metric_values_by_candidate: Dict[str, Dict[str, float]] = {}
    invalid_metric_candidates = 0
    for row in structural_rows:
        cid = str(row.get("candidate_id", ""))
        raw_metrics = dict(row.get("metrics") or {})
        metrics = {aliases.get(str(k), str(k)): v for k, v in raw_metrics.items()}
        vals: Dict[str, float] = {}
        valid = True
        for key in required_metrics:
            fv = _finite_number(metrics.get(key))
            if fv is None:
                missing_counts[key] = int(missing_counts.get(key, 0)) + 1
                valid = False
            else:
                vals[key] = fv
        if valid:
            metric_values_by_candidate[cid] = vals
            for key in required_metrics:
                valid_metric_values[key].append(vals[key])
        else:
            invalid_metric_candidates += 1

    valid_ids = sorted(metric_values_by_candidate.keys())
    structural_count = len(structural_rows)
    valid_count = len(valid_ids)
    invalid_ratio = (float(invalid_metric_candidates) / float(structural_count)) if structural_count > 0 else 1.0

    hard_fail = False
    hard_fail_reason = None
    if structural_count == 0:
        hard_fail = True
        hard_fail_reason = "no_structural_pass_candidates"
    elif invalid_ratio >= 0.6:
        hard_fail = True
        hard_fail_reason = "metrics_missing_or_nonfinite_dominant"
    elif valid_count == 0:
        hard_fail = True
        hard_fail_reason = "no_valid_candidates_with_finite_required_metrics"

    composite_rank: Dict[str, float] = {}
    fallback_selected_ids: set[str] = set()
    dynamic_pass_ids: set[str] = set()
    fallback_used = False

    if not hard_fail:
        per_metric_percentiles: Dict[str, Dict[str, float]] = {}
        weight_sum = 0.0
        for key in required_metrics:
            rule = metric_rules.get(key, MetricRule(direction="high", weight=0.0))
            weight_sum += max(0.0, float(rule.weight))
            values = {cid: metric_values_by_candidate[cid][key] for cid in valid_ids}
            per_metric_percentiles[key] = _percentile_by_order(valid_ids, values, direction=rule.direction)
        if weight_sum <= 0.0:
            hard_fail = True
            hard_fail_reason = "invalid_metric_weights"
        else:
            for cid in valid_ids:
                score = 0.0
                for key in required_metrics:
                    rule = metric_rules.get(key, MetricRule(direction="high", weight=0.0))
                    score += float(per_metric_percentiles[key][cid]) * max(0.0, float(rule.weight))
                composite_rank[cid] = score / weight_sum
                if composite_rank[cid] >= float(config.top_percentile):
                    dynamic_pass_ids.add(cid)
            if len(dynamic_pass_ids) == 0 and valid_count < int(config.min_population):
                top_k = max(0, int(config.fallback_top_k_if_empty))
                if top_k > 0:
                    ordered = sorted(valid_ids, key=lambda c: (-float(composite_rank[c]), str(c)))
                    fallback_selected_ids = set(ordered[:top_k])
                    dynamic_pass_ids = set(fallback_selected_ids)
                    fallback_used = len(fallback_selected_ids) > 0

    for row in sorted(candidate_rows, key=lambda x: str(x.get("candidate_id", ""))):
        cid = str(row.get("candidate_id", ""))
        structural_pass = bool(row.get("structural_pass", False))
        static_pass = bool(row.get("static_pass", False))
        base_reasons = list(row.get("reasons") or [])
        row_reasons = list(base_reasons)
        metric_valid = cid in metric_values_by_candidate
        dyn_pass = bool(cid in dynamic_pass_ids) if not hard_fail else False
        final_pass = bool(structural_pass and metric_valid and dyn_pass and not hard_fail)

        reject_reason_code = ""
        if not structural_pass:
            row_reasons.append("structural_reject")
            _count("structural_reject")
            reject_reason_code = "structural_reject"
        if structural_pass and not metric_valid:
            row_reasons.append("missing_required_metric")
            _count("missing_required_metric")
            reject_reason_code = "missing_required_metric"
        if structural_pass and metric_valid and not dyn_pass and not hard_fail:
            row_reasons.append("dynamic_reject")
            _count("dynamic_reject")
            reject_reason_code = "dynamic_reject"
        if structural_pass and static_pass is False:
            _count("static_reject")
        if final_pass:
            _count("final_pass")
            reject_reason_code = ""
        if cid in fallback_selected_ids:
            row_reasons.append("fallback_selected")
            _count("fallback_selected")
        if (not final_pass) and (not reject_reason_code) and hard_fail and hard_fail_reason:
            reject_reason_code = str(hard_fail_reason)

        decisions.append(
            {
                "candidate_id": cid,
                "structural_pass": structural_pass,
                "static_pass": static_pass,
                "dynamic_pass": dyn_pass,
                "final_pass": final_pass,
                "composite_rank": composite_rank.get(cid),
                "reasons": row_reasons,
                "fallback_selected": bool(cid in fallback_selected_ids),
                "reject_reason_code": reject_reason_code,
            }
        )

    quantiles: Dict[str, Dict[str, float | None]] = {}
    for key in required_metrics:
        vals = sorted(valid_metric_values.get(key, []))
        if not vals:
            quantiles[key] = {"q0": None, "q25": None, "q50": None, "q75": None, "q100": None}
            continue
        n = len(vals)
        def q(p: float) -> float:
            if n == 1:
                return float(vals[0])
            idx = (n - 1) * p
            lo = int(math.floor(idx))
            hi = int(math.ceil(idx))
            if lo == hi:
                return float(vals[lo])
            w = idx - lo
            return float(vals[lo] * (1.0 - w) + vals[hi] * w)
        quantiles[key] = {
            "q0": float(vals[0]),
            "q25": q(0.25),
            "q50": q(0.50),
            "q75": q(0.75),
            "q100": float(vals[-1]),
        }

    return {
        "ok": not hard_fail,
        "hard_fail": bool(hard_fail),
        "hard_fail_reason": hard_fail_reason,
        "fallback_used": bool(fallback_used),
        "thresholds": {
            "enabled": True,
            "method": str(config.method),
            "top_percentile": float(config.top_percentile),
            "min_population": int(config.min_population),
            "fallback_top_k_if_empty": int(config.fallback_top_k_if_empty),
            "required_metrics": list(required_metrics),
            "metric_rules": {
                k: {"direction": str(v.direction), "weight": float(v.weight)} for k, v in metric_rules.items()
            },
        },
        "population_stats": {
            "total_candidates": len(candidate_rows),
            "structural_pass_count": structural_count,
            "valid_metric_candidates": valid_count,
            "invalid_metric_candidates": invalid_metric_candidates,
            "invalid_ratio": invalid_ratio,
            "missing_metric_counts": missing_counts,
            "metric_quantiles": quantiles,
        },
        "reason_counts": reason_counts,
        "decisions": decisions,
    }
