"""Crowding & concentration risk utilities.

Provides an exposure graph and concentration metrics (HHI, top-N), duplicate
detection and recommendations for allocator/sentinel actions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class Node:
    id: str
    type: str  # 'strategy' | 'asset' | 'asset_class'


class ExposureGraph:
    def __init__(self):
        # adjacency: node_id -> dict[target_id->weight]
        self._adj: Dict[str, Dict[str, float]] = {}
        self._nodes: Dict[str, Node] = {}

    def add_node(self, node_id: str, node_type: str):
        self._nodes[node_id] = Node(node_id, node_type)
        self._adj.setdefault(node_id, {})

    def add_edge(self, src: str, dst: str, weight: float):
        self.add_node(src, "strategy" if src.startswith("s:") else "asset")
        self.add_node(dst, "asset" if dst.startswith("a:") else "asset_class")
        self._adj.setdefault(src, {})
        self._adj[src][dst] = float(weight)

    def strategy_exposures(self, strategy_id: str) -> Dict[str, float]:
        return dict(self._adj.get(strategy_id, {}))

    def asset_totals(self) -> Dict[str, float]:
        totals: Dict[str, float] = {}
        for s, edges in self._adj.items():
            if not s.startswith("s:"):
                continue
            for a, w in edges.items():
                totals[a] = totals.get(a, 0.0) + abs(w)
        return totals

    def all_strategies(self) -> List[str]:
        return [n for n, meta in self._nodes.items() if meta.type == "strategy"]

    def serialize(self) -> str:
        return json.dumps(
            {"nodes": {k: v.type for k, v in self._nodes.items()}, "adj": self._adj}
        )


def herfindahl_index(values: List[float]) -> float:
    total = sum(values)
    if total <= 0:
        return 0.0
    shares = [(abs(v) / total) for v in values]
    return float(sum([s * s for s in shares]))


def top_n_contribution(
    values_map: Dict[str, float], n: int = 3
) -> Tuple[List[Tuple[str, float]], float]:
    items = sorted(values_map.items(), key=lambda x: -abs(x[1]))
    top = items[:n]
    total = sum(abs(v) for _, v in values_map.items())
    top_sum = sum(abs(v) for _, v in top)
    share = float(top_sum / total) if total > 0 else 0.0
    return top, share


def detect_duplicate_strategies(
    graph: ExposureGraph, similarity_threshold: float = 0.8
) -> List[Tuple[str, str, float]]:
    # Build strategy exposure vectors over the union of assets
    strategies = graph.all_strategies()
    if len(strategies) < 2:
        return []
    assets = sorted({a for s in strategies for a in graph.strategy_exposures(s).keys()})
    import numpy as np

    mat = []
    for s in strategies:
        vec = [graph.strategy_exposures(s).get(a, 0.0) for a in assets]
        mat.append(vec)
    arr = np.array(mat)
    # cosine similarity
    sims = []
    norms = (arr**2).sum(axis=1) ** 0.5
    for i in range(arr.shape[0]):
        for j in range(i + 1, arr.shape[0]):
            denom = norms[i] * norms[j]
            sim = float((arr[i] @ arr[j]) / denom) if denom > 0 else 0.0
            if sim >= similarity_threshold:
                sims.append((strategies[i], strategies[j], sim))
    return sims


def factor_proxy_concentration(
    graph: ExposureGraph,
    factor_map: Dict[str, Dict[str, float]],
    conservative_cap: float = 0.05,
) -> Dict[str, Dict]:
    """Compute concentration per factor proxy.

    factor_map: factor -> {asset_id: loading}
    Returns dict factor -> {hhi, top_n_share, recommended_cap}
    """
    results = {}
    graph.asset_totals()
    for factor, loading_map in factor_map.items():
        # compute exposure to factor as sum(abs(exposure*loading)) across assets
        factor_exposures: Dict[str, float] = {}
        for s in graph.all_strategies():
            exposures = graph.strategy_exposures(s)
            val = 0.0
            for a, w in exposures.items():
                loading = loading_map.get(a)
                if loading is None:
                    # if missing proxy treat as zero for this factor
                    continue
                val += abs(w * loading)
            factor_exposures[s] = val
        hhi = herfindahl_index(list(factor_exposures.values()))
        top_items, top_share = top_n_contribution(factor_exposures, n=3)
        # recommended cap: if top_share large, advise compression
        if top_share > 0.4:
            recommended = max(0.1, 1.0 - (top_share - 0.4))
        else:
            recommended = 1.0
        results[factor] = {
            "hhi": hhi,
            "top_share": top_share,
            "top": top_items,
            "recommended": recommended,
        }

    # For missing factors, enforce conservative caps globally
    if not factor_map:
        results["__missing_proxies__"] = {"recommended": conservative_cap}

    return results


def evaluate_concentration(
    graph: ExposureGraph,
    factor_map: Optional[Dict[str, Dict[str, float]]] = None,
    thresholds: Dict[str, float] | None = None,
) -> Dict:
    if thresholds is None:
        thresholds = {"hhi_asset": 0.08, "topn_share": 0.35, "dup_similarity": 0.85}
    assets = graph.asset_totals()
    hhi = herfindahl_index(list(assets.values()))
    topn, topn_share = top_n_contribution(assets, n=5)

    duplicates = detect_duplicate_strategies(
        graph, similarity_threshold=thresholds.get("dup_similarity", 0.85)
    )

    factor_results = factor_proxy_concentration(graph, factor_map or {})

    actions: Dict[str, Any] = {"scale_recommendations": {}, "sentinel": []}
    # If asset-level HHI exceeds threshold, recommend compression
    if hhi > thresholds["hhi_asset"]:
        # compression proportional to excess
        factor = max(0.1, 1.0 - min(0.9, (hhi - thresholds["hhi_asset"]) * 10))
        actions["scale_recommendations"]["global"] = factor
        actions["sentinel"].append({"level": 2, "reason": "asset_hhi_exceeded"})

    # Duplicate strategy handling
    for s1, s2, sim in duplicates:
        actions["scale_recommendations"][s1] = 0.5
        actions["scale_recommendations"][s2] = 0.5
        actions["sentinel"].append(
            {"level": 3, "reason": f"duplicate_strategies:{s1}:{s2}", "similarity": sim}
        )

    # Factor caps recommendations
    for f, info in factor_results.items():
        if f == "__missing_proxies__":
            actions["scale_recommendations"]["global"] = (
                min(
                    actions["scale_recommendations"].get("global", 1.0),
                    info["recommended"],
                )
                if isinstance(info, dict)
                else info
            )
        else:
            if info["top_share"] > thresholds["topn_share"]:
                actions["scale_recommendations"][f] = info["recommended"]
                actions["sentinel"].append(
                    {"level": 2, "reason": f"factor_concentration:{f}"}
                )

    return {
        "hhi_asset": hhi,
        "topn": topn,
        "duplicates": duplicates,
        "factor_results": factor_results,
        "actions": actions,
    }
