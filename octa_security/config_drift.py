from __future__ import annotations

import datetime
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class ConfigDriftException(Exception):
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


@dataclass
class ConfigBaseline:
    baseline: Dict[str, Any]
    # allowed numeric ranges for parameters: key -> (min, max)
    allowed_ranges: Dict[str, Tuple[Optional[float], Optional[float]]] = field(
        default_factory=dict
    )
    # approvals required to accept config changes (hash mismatches)
    required_approvals: Set[str] = field(default_factory=set)
    created_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.utcnow()
    )
    baseline_hash: str = field(init=False)
    audit_log: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self):
        self.baseline_hash = canonical_hash(self.baseline)
        self.record_audit(
            actor="system",
            action="baseline_initialized",
            details={"baseline_hash": self.baseline_hash},
        )

    def record_audit(self, actor: str, action: str, details: Dict[str, Any]):
        entry = {
            "ts": datetime.datetime.utcnow().isoformat() + "Z",
            "actor": actor,
            "action": action,
            "details": details,
        }
        self.audit_log.append(entry)

    def compare(self, current_config: Dict[str, Any]) -> Dict[str, Any]:
        current_hash = canonical_hash(current_config)
        hash_mismatch = current_hash != self.baseline_hash

        param_violations: List[Dict[str, Any]] = []

        # Check numeric boundary violations where applicable
        for key, (min_v, max_v) in self.allowed_ranges.items():
            if key in current_config:
                try:
                    val = float(current_config[key])
                except Exception:
                    # cannot cast to float -> violation
                    param_violations.append(
                        {
                            "param": key,
                            "baseline": self.baseline.get(key),
                            "current": current_config.get(key),
                            "reason": "non_numeric",
                        }
                    )
                    continue

                if min_v is not None and val < min_v:
                    param_violations.append(
                        {
                            "param": key,
                            "baseline": self.baseline.get(key),
                            "current": val,
                            "reason": "below_min",
                            "min": min_v,
                        }
                    )
                if max_v is not None and val > max_v:
                    param_violations.append(
                        {
                            "param": key,
                            "baseline": self.baseline.get(key),
                            "current": val,
                            "reason": "above_max",
                            "max": max_v,
                        }
                    )

        # Additionally, detect unexpected parameter value changes (non-boundary)
        unexpected_changes: List[Dict[str, Any]] = []
        for k, v in self.baseline.items():
            if k not in current_config:
                unexpected_changes.append(
                    {"param": k, "reason": "missing_in_current", "baseline": v}
                )
            else:
                if current_config[k] != v and k not in self.allowed_ranges:
                    unexpected_changes.append(
                        {
                            "param": k,
                            "baseline": v,
                            "current": current_config[k],
                            "reason": "value_changed",
                        }
                    )

        return {
            "baseline_hash": self.baseline_hash,
            "current_hash": current_hash,
            "hash_mismatch": hash_mismatch,
            "param_violations": param_violations,
            "unexpected_changes": unexpected_changes,
        }

    def enforce(
        self,
        current_config: Dict[str, Any],
        approvals: Optional[List[str]] = None,
        actor: str = "operator",
    ) -> bool:
        approvals_set: Set[str] = set(approvals or [])
        report = self.compare(current_config)

        # If parameter boundary violations -> always block
        if report["param_violations"]:
            self.record_audit(
                actor=actor,
                action="drift_blocked_param_violation",
                details={"report": report},
            )
            raise ConfigDriftException(
                "Parameter boundary violation detected", details=report
            )

        # If hash mismatch -> require approvals
        if report["hash_mismatch"]:
            missing = list(self.required_approvals - approvals_set)
            if missing:
                self.record_audit(
                    actor=actor,
                    action="drift_blocked_missing_approvals",
                    details={"missing_approvals": missing, "report": report},
                )
                raise ConfigDriftException(
                    "Config hash mismatch and required approvals missing",
                    details={"missing_approvals": missing, "report": report},
                )
            # approvals present -> accept but record explicit approval
            self.record_audit(
                actor=actor,
                action="drift_accepted_with_approvals",
                details={"approvals": list(approvals_set), "report": report},
            )
            return True

        # No drift detected
        self.record_audit(actor=actor, action="drift_none", details={"report": report})
        return True
