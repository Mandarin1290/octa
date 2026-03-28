from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping


DEFAULT_METRIC_GOVERNANCE_POLICY: dict[str, Any] = {
    "non_finite_metrics": {
        "default": {
            "allowed_raw_values": [],
            "handling": "block",
            "cap_value": None,
            "require_reason_annotation": True,
            "allowed_annotations": [],
        },
        "profit_factor": {
            "allowed_raw_values": [],
            "handling": "block",
            "cap_value": 999.0,
            "require_reason_annotation": True,
            "allowed_annotations": ["profit_factor_infinite_due_to_zero_gross_loss"],
        },
    },
    "nan_metrics": {
        "default": {
            "handling": "block",
            "require_reason_annotation": False,
        }
    },
    "division_by_zero_cases": {
        "profit_factor": {
            "zero_gross_loss_annotation": "profit_factor_infinite_due_to_zero_gross_loss",
            "zero_gross_profit_annotation": "profit_factor_zero_due_to_zero_gross_profit",
        }
    },
}


def default_metric_governance_policy() -> dict[str, Any]:
    return deepcopy(DEFAULT_METRIC_GOVERNANCE_POLICY)


def resolve_metric_governance_policy(
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    resolved = default_metric_governance_policy()
    if payload is None:
        return resolved
    for top_level_key, top_level_value in payload.items():
        if isinstance(top_level_value, Mapping) and isinstance(resolved.get(top_level_key), dict):
            resolved_section = deepcopy(resolved[top_level_key])
            for section_key, section_value in top_level_value.items():
                if isinstance(section_value, Mapping) and isinstance(resolved_section.get(section_key), dict):
                    merged = deepcopy(resolved_section[section_key])
                    merged.update(dict(section_value))
                    resolved_section[section_key] = merged
                else:
                    resolved_section[section_key] = section_value
            resolved[top_level_key] = resolved_section
        else:
            resolved[top_level_key] = top_level_value
    return resolved


__all__ = [
    "DEFAULT_METRIC_GOVERNANCE_POLICY",
    "default_metric_governance_policy",
    "resolve_metric_governance_policy",
]
