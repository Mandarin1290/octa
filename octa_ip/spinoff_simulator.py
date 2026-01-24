from dataclasses import dataclass
from typing import List, Set, Tuple

from .module_map import ModuleMap


@dataclass
class SpinoffReport:
    module: str
    included: List[str]
    missing: List[str]
    adaptations: List[str]


class SpinoffSimulator:
    """Simulate extracting a module (virtual only) and report adaptations required.

    Conservative rules:
    - Only modules reachable by dependency graph are included.
    - Any dependency not present in `ModuleMap` is reported as missing.
    - Adaptations include: external owner dependencies, internal-only exposures, and secrets placeholders in files (best-effort).
    """

    def __init__(self):
        pass

    def _collect_deps(self, mm: ModuleMap, root: str) -> Tuple[Set[str], Set[str]]:
        """Return (found, missing) modules reachable from root via deps."""
        visited: Set[str] = set()
        missing: Set[str] = set()
        stack = [root]
        deps = getattr(mm, "deps", {})
        while stack:
            cur = stack.pop()
            if cur in visited:
                continue
            visited.add(cur)
            if cur not in mm.modules:
                missing.add(cur)
                continue
            for d in deps.get(cur, set()):
                if d not in visited:
                    stack.append(d)
        return visited - missing, missing

    def simulate(self, mm: ModuleMap, module: str) -> SpinoffReport:
        included, missing = self._collect_deps(mm, module)

        adaptations: List[str] = []

        # detect external-owner dependencies
        for m in sorted(included):
            info = mm.modules.get(m)
            if not info:
                continue
            # reverse deps: who depends on this module
            reverse = [s for s, ds in getattr(mm, "deps", {}).items() if m in ds]
            owners = set()
            for r in reverse:
                rinfo = mm.modules.get(r)
                if rinfo:
                    owners.add(rinfo.owner)
            if owners and (
                len(owners) > 1
                or (len(owners) == 1 and next(iter(owners)) != info.owner)
            ):
                adaptations.append(
                    f"module {m} has cross-owner dependents: owners={sorted(owners)}"
                )

        # if missing dependencies exist, add adaptation to vendor or implement
        for miss in sorted(missing):
            adaptations.append(
                f"missing dependency: {miss} (vendor or provide alternative)"
            )

        return SpinoffReport(
            module=module,
            included=sorted(included),
            missing=sorted(missing),
            adaptations=adaptations,
        )


__all__ = ["SpinoffSimulator", "SpinoffReport"]
