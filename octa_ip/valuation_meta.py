from dataclasses import dataclass
from typing import Any, Dict, Set

from .ip_classifier import LICENSABLE
from .module_map import ModuleMap


@dataclass
class ValuationMetrics:
    module: str
    usage_frequency: float
    dependency_centrality: float
    risk_criticality: float
    revenue_relevance: float = 0.0


class ValuationEngine:
    """Compute evidence-based valuation metadata for modules.

    Metrics are deterministic given the same inputs.
    """

    def compute_metadata(
        self,
        mm: ModuleMap,
        usage_counts: Dict[str, int],
        revenue_map: Dict[str, float] | None = None,
    ) -> Dict[str, Dict[str, Any]]:
        modules = sorted(mm.modules.keys())
        n = max(1, len(modules))

        # total usage for normalization
        total_usage = max(1, sum(usage_counts.get(m, 0) for m in modules))

        # compute indegree (how many modules depend on this module)
        indeg = {m: 0 for m in modules}
        outdeg = {m: 0 for m in modules}
        deps = getattr(mm, "deps", {})
        for src in modules:
            for dst in sorted(deps.get(src, set())):
                if dst in indeg:
                    indeg[dst] += 1
                    outdeg[src] += 1

        # owner diversity: count distinct owners that depend on the module
        owner_deps: Dict[str, Set[str]] = {m: set() for m in modules}
        for src in modules:
            for dst in deps.get(src, set()):
                if dst in owner_deps and src in mm.modules:
                    owner_deps[dst].add(mm.modules[src].owner)

        result: Dict[str, Dict[str, Any]] = {}
        for m in modules:
            usage_freq = usage_counts.get(m, 0) / total_usage

            # dependency centrality: normalized indegree
            dependency_centrality = indeg[m] / max(1, (n - 1))

            # risk criticality: indegree weighted by owner diversity and usage
            owner_diversity = len(owner_deps.get(m, set()))
            risk_criticality = dependency_centrality * (
                1.0 + owner_diversity / max(1, n)
            )
            # incorporate usage frequency conservatively
            risk_criticality = risk_criticality * (1.0 + usage_freq)

            revenue_relevance = 0.0
            # evidence-based: only consider revenue if provided and module classified as licensable
            try:
                classification = mm.modules[m].classification
            except Exception:
                classification = None
            if revenue_map and m in revenue_map:
                # accept revenue only if classification indicates licensable
                if classification and (
                    str(classification).lower() == "licensable"
                    or str(classification) == LICENSABLE
                ):
                    revenue_relevance = float(revenue_map[m])

            result[m] = {
                "usage_frequency": usage_freq,
                "dependency_centrality": dependency_centrality,
                "risk_criticality": risk_criticality,
                "revenue_relevance": revenue_relevance,
            }

        return result


__all__ = ["ValuationEngine", "ValuationMetrics"]
