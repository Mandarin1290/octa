from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

from octa_ip.module_map import ModuleMap

CORE_PROPRIETARY = "CORE_PROPRIETARY"
LICENSABLE = "LICENSABLE"
INTERNAL_ONLY = "INTERNAL_ONLY"
OPEN_SOURCE_DERIVED = "OPEN_SOURCE_DERIVED"


@dataclass
class IPClassifier:
    classifications: Dict[str, str] = field(default_factory=dict)  # module -> category
    license_constraints: Dict[str, Set[str]] = field(default_factory=dict)

    def __post_init__(self):
        # default constraints disallow certain dst categories from being imported into src category
        # mapping: src_category -> forbidden dst categories
        self.license_constraints = {
            CORE_PROPRIETARY: {OPEN_SOURCE_DERIVED},
            LICENSABLE: set(),
            INTERNAL_ONLY: set(),
            OPEN_SOURCE_DERIVED: set(),
        }

    def set_classification(self, module: str, category: str) -> None:
        if category not in {
            CORE_PROPRIETARY,
            LICENSABLE,
            INTERNAL_ONLY,
            OPEN_SOURCE_DERIVED,
        }:
            raise ValueError("unknown category")
        self.classifications[module] = category

    def classify_from_module_map(self, mm: ModuleMap) -> None:
        # adopt UNKNOWN modules from ModuleMap as INTERNAL_ONLY by default unless set
        for name in mm.modules:
            if name not in self.classifications:
                self.classifications[name] = INTERNAL_ONLY

    def validate(self, mm: ModuleMap) -> List[Tuple[str, str, str]]:
        """Return list of violations (src_module, dst_module, reason).

        Enforced rules:
        - If src classified CORE_PROPRIETARY, it must not depend on any OPEN_SOURCE_DERIVED modules.
        - No module may depend on an `INTERNAL_ONLY` module owned by a different owner (checked via ModuleMap.detect_violations would catch cross-owner-internal).
        - All modules must have an explicit classification.
        """
        violations = []
        # ensure all modules classified
        for m in mm.modules:
            if m not in self.classifications:
                violations.append((m, "", "unclassified"))

        # check dependency constraints
        for src, dsts in mm.deps.items():
            src_cat = self.classifications.get(src)
            for dst in dsts:
                dst_cat = self.classifications.get(dst)
                if dst_cat is None:
                    violations.append((src, dst, "dst-unclassified"))
                    continue
                # rule: CORE cannot depend on OPEN_SOURCE_DERIVED
                if src_cat == CORE_PROPRIETARY and dst_cat == OPEN_SOURCE_DERIVED:
                    violations.append((src, dst, "core-depends-on-open-source-derived"))

        # include ModuleMap level violations (cross-owner-internal)
        mm_viol = mm.detect_violations()
        for v in mm_viol:
            if v[2] == "cross-owner-internal":
                violations.append((v[0], v[1], "cross-owner-internal"))

        return violations

    def enforce_runtime(self, mm: ModuleMap) -> None:
        violations = self.validate(mm)
        if violations:
            # raise exception summarizing violations
            msgs = [f"{s} -> {d}: {r}" for s, d, r in violations]
            raise RuntimeError("IP classification violations:\n" + "\n".join(msgs))


__all__ = [
    "IPClassifier",
    "CORE_PROPRIETARY",
    "LICENSABLE",
    "INTERNAL_ONLY",
    "OPEN_SOURCE_DERIVED",
]
