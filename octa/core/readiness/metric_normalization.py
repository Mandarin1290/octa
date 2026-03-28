from __future__ import annotations

import math
from typing import Any, Mapping

from .metric_governance_policy import resolve_metric_governance_policy


def _metric_rule(policy: Mapping[str, Any], metric_name: str, section: str) -> dict[str, Any]:
    section_payload = policy.get(section, {})
    if not isinstance(section_payload, Mapping):
        return {}
    specific = section_payload.get(metric_name)
    if isinstance(specific, Mapping):
        return dict(specific)
    default = section_payload.get("default")
    return dict(default) if isinstance(default, Mapping) else {}


def normalize_readiness_metrics(
    metrics: Mapping[str, Any],
    policy: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_policy = resolve_metric_governance_policy(policy)
    raw = dict(metrics)
    normalized = dict(metrics)
    flags: list[str] = []
    annotations: list[str] = []
    decisions: list[dict[str, Any]] = []
    classification = "acceptable_with_caveat"
    decision_reason = "all metrics finite or explicitly acceptable"

    for name, value in raw.items():
        if not isinstance(value, (int, float)):
            continue
        numeric = float(value)
        if math.isnan(numeric):
            rule = _metric_rule(resolved_policy, name, "nan_metrics")
            flags.append(f"{name}_nan")
            annotation = f"{name}_nan_raw"
            annotations.append(annotation)
            handling = str(rule.get("handling", "block"))
            normalized[name] = None
            decisions.append(
                {
                    "metric": name,
                    "raw_value": value,
                    "normalized_value": None,
                    "classification": "blocking",
                    "handling": handling,
                    "reason": annotation,
                }
            )
            classification = "blocking"
            decision_reason = f"{name} is NaN and policy handling is {handling}"
            continue

        if math.isinf(numeric):
            rule = _metric_rule(resolved_policy, name, "non_finite_metrics")
            handling = str(rule.get("handling", "block"))
            annotation = f"{name}_{'positive' if numeric > 0 else 'negative'}_infinity_raw"
            division_cases = resolved_policy.get("division_by_zero_cases", {})
            division_rule = division_cases.get(name, {}) if isinstance(division_cases, Mapping) else {}
            if (
                name == "profit_factor"
                and numeric > 0
                and "gross_loss" in raw
                and isinstance(raw["gross_loss"], (int, float))
                and float(raw["gross_loss"]) == 0.0
            ):
                annotation = str(
                    division_rule.get(
                        "zero_gross_loss_annotation",
                        "profit_factor_infinite_due_to_zero_gross_loss",
                    )
                )
            elif (
                name == "profit_factor"
                and numeric == 0.0
                and "gross_profit" in raw
                and isinstance(raw["gross_profit"], (int, float))
                and float(raw["gross_profit"]) == 0.0
            ):
                annotation = str(
                    division_rule.get(
                        "zero_gross_profit_annotation",
                        "profit_factor_zero_due_to_zero_gross_profit",
                    )
                )

            flags.append(f"{name}_non_finite")
            annotations.append(annotation)
            allowed_annotations = rule.get("allowed_annotations", [])
            require_reason = bool(rule.get("require_reason_annotation", False))

            if handling == "cap_and_flag":
                normalized_value = rule.get("cap_value")
                normalized[name] = normalized_value
                metric_classification = (
                    "normalized_with_flag"
                    if (not require_reason or annotation in allowed_annotations)
                    else "blocking"
                )
            elif handling == "manual_review":
                normalized[name] = None
                metric_classification = "acceptable_with_caveat"
            else:
                normalized[name] = None
                metric_classification = "blocking"

            decisions.append(
                {
                    "metric": name,
                    "raw_value": value,
                    "normalized_value": normalized.get(name),
                    "classification": metric_classification,
                    "handling": handling,
                    "reason": annotation,
                }
            )
            if metric_classification == "blocking":
                classification = "blocking"
                decision_reason = (
                    f"{name} is non-finite and policy handling '{handling}' "
                    f"does not explicitly permit annotation '{annotation}'"
                )
            elif classification != "blocking" and metric_classification == "normalized_with_flag":
                classification = "normalized_with_flag"
                decision_reason = f"{name} normalized under explicit cap_and_flag policy"
            elif classification == "acceptable_with_caveat":
                decision_reason = f"{name} requires explicit manual review handling"

    return {
        "raw": raw,
        "normalized": normalized,
        "flags": flags,
        "annotations": annotations,
        "decisions": decisions,
        "classification": classification,
        "policy_decision_reason": decision_reason,
        "policy": resolved_policy,
    }


__all__ = ["normalize_readiness_metrics"]
