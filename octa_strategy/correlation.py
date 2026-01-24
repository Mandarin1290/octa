from math import sqrt
from typing import Dict, List, Tuple


def _pearson(x: List[float], y: List[float]) -> float:
    n = min(len(x), len(y))
    if n < 2:
        return 0.0
    xm = sum(x[:n]) / n
    ym = sum(y[:n]) / n
    cov = sum((x[i] - xm) * (y[i] - ym) for i in range(n))
    varx = sum((x[i] - xm) ** 2 for i in range(n))
    vary = sum((y[i] - ym) ** 2 for i in range(n))
    denom = sqrt(varx * vary)
    if denom == 0:
        return 0.0
    return cov / denom


class StrategyCorrelation:
    """Compute rolling correlation matrix, redundancy scores and compress budgets.

    - `assess(returns_by_strategy)` returns per-strategy redundancy score (mean absolute correlation to others), flagging those above `redundancy_threshold`.
    - `compress_budgets(budgets, report)` returns adjusted budgets applying compression = max(0.1, 1 - score).
    """

    def __init__(
        self, redundancy_threshold: float = 0.8, audit_fn=None, sentinel_api=None
    ):
        self.redundancy_threshold = redundancy_threshold
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.sentinel_api = sentinel_api

    def _compute_matrix(
        self, returns: Dict[str, List[float]]
    ) -> Tuple[List[str], List[List[float]]]:
        keys = list(returns.keys())
        n = len(keys)
        mat = [[0.0] * n for _ in range(n)]
        for i in range(n):
            for j in range(n):
                mat[i][j] = _pearson(returns[keys[i]], returns[keys[j]])
        return keys, mat

    def assess(self, returns: Dict[str, List[float]]) -> Dict[str, Dict]:
        keys, mat = self._compute_matrix(returns)
        n = len(keys)
        report: Dict[str, Dict] = {}
        for i, k in enumerate(keys):
            # redundancy score: max absolute correlation to any other strategy
            if n <= 1:
                score = 0.0
            else:
                m = 0.0
                for j in range(n):
                    if i == j:
                        continue
                    m = max(m, abs(mat[i][j]))
                score = m
            flagged = score >= self.redundancy_threshold
            compression = max(0.1, 1.0 - score)
            report[k] = {"score": score, "flagged": flagged, "compression": compression}
            if flagged:
                try:
                    if self.sentinel_api is not None:
                        self.sentinel_api.set_gate(
                            2, f"redundant_strategy:{k}:score={score:.3f}"
                        )
                except Exception:
                    pass
        self.audit_fn("correlation.assess", {"report": report})
        return report

    def compress_budgets(
        self, budgets: Dict[str, float], report: Dict[str, Dict]
    ) -> Dict[str, float]:
        adjusted = {}
        for k, b in budgets.items():
            r = report.get(k)
            if not r:
                adjusted[k] = b
                continue
            adj = b * r.get("compression", 1.0)
            adjusted[k] = adj
            self.audit_fn(
                "correlation.compress",
                {"strategy_id": k, "old": b, "new": adj, "score": r.get("score")},
            )
        return adjusted
