from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

from .ip_classifier import (
    CORE_PROPRIETARY,
    INTERNAL_ONLY,
    LICENSABLE,
    OPEN_SOURCE_DERIVED,
    IPClassifier,
)
from .module_map import ModuleMap


@dataclass
class PolicyViolation:
    module: str
    violation: str
    details: Dict[str, Any]


class PolicyEnforcer:
    """Enforce license & usage policies at runtime.

    Rules are code-enforced. Any violation halts execution (RuntimeError)
    and is recorded in `audit_log`.
    """

    def __init__(self):
        self.audit_log: List[Dict[str, Any]] = []

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _log(self, level: str, msg: str, meta: Dict[str, Any]):
        entry = {"ts": self._now(), "level": level, "msg": msg, "meta": meta}
        self.audit_log.append(entry)

    def policy_rules(self) -> Dict[str, Dict[str, Any]]:
        """Return policy rules per IP category.

        The structure is simple and extensible. Keys used:
        - `forbidden_deps`: list of IPCategory values that a module of this
          category must not depend on.
        - `forbidden_external_use`: if True, modules in this category must
          not be imported by modules owned by other owners.
        """

        return {
            CORE_PROPRIETARY: {
                "forbidden_deps": [OPEN_SOURCE_DERIVED],
            },
            LICENSABLE: {
                "forbidden_deps": [],
            },
            INTERNAL_ONLY: {
                "forbidden_external_use": True,
            },
            OPEN_SOURCE_DERIVED: {
                "forbidden_deps": [CORE_PROPRIETARY],
            },
        }

    def check_policies(
        self, module_map: ModuleMap, classifier: IPClassifier
    ) -> List[PolicyViolation]:
        """Check policies and return a list of violations (does not raise)."""

        rules = self.policy_rules()
        violations: List[PolicyViolation] = []

        module_categories: Dict[str, str] = {}
        # Ensure classifier has its classifications populated; fall back to classify_from_module_map
        if not getattr(classifier, "classifications", None):
            try:
                classifier.classify_from_module_map(module_map)
            except Exception:
                pass

        for mname in module_map.modules:
            module_categories[mname] = classifier.classifications.get(mname) or ""

        for mname, _minfo in module_map.modules.items():
            cat = module_categories[mname]
            rule = rules.get(cat, {})

            forbidden = rule.get("forbidden_deps", [])
            deps = getattr(module_map, "deps", {})
            for dep in deps.get(mname, set()):
                dep_cat = module_categories.get(dep)
                if dep_cat in forbidden:
                    violations.append(
                        PolicyViolation(
                            module=mname,
                            violation="forbidden_dependency",
                            details={
                                "depends_on": dep,
                                "dep_category": dep_cat,
                                "module_category": cat,
                            },
                        )
                    )

            if rule.get("forbidden_external_use"):
                for other_name, other_info in module_map.modules.items():
                    other_deps = getattr(module_map, "deps", {}).get(other_name, set())
                    if (
                        mname in other_deps
                        and other_info.owner != module_map.modules[mname].owner
                    ):
                        violations.append(
                            PolicyViolation(
                                module=mname,
                                violation="external_use_of_internal",
                                details={
                                    "used_by": other_name,
                                    "user_owner": other_info.owner,
                                    "internal_owner": module_map.modules[mname].owner,
                                },
                            )
                        )

        return violations

    def enforce(self, module_map: ModuleMap, classifier: IPClassifier):
        """Enforce policies at runtime. Logs violations and raises if any present."""

        violations = self.check_policies(module_map, classifier)
        if not violations:
            self._log(
                "info",
                "policy_check_passed",
                {"modules_checked": len(module_map.modules)},
            )
            return

        for v in violations:
            self._log(
                "error",
                "policy_violation",
                {"module": v.module, "violation": v.violation, "details": v.details},
            )

        # Hard rule: halt execution on any policy violation
        raise RuntimeError(
            f"PolicyEnforcer detected {len(violations)} violations; execution halted."
        )


__all__ = ["PolicyEnforcer", "PolicyViolation"]
