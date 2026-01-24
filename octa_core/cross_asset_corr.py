from math import sqrt
from typing import Dict, List, Tuple


def _mean_abs_offdiag(corr_matrix: List[List[float]]) -> float:
    n = len(corr_matrix)
    if n <= 1:
        return 0.0
    s = 0.0
    cnt = 0
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            s += abs(corr_matrix[i][j])
            cnt += 1
    return s / cnt if cnt else 0.0


class CrossAssetCorrelation:
    """Cross-asset correlation regimes and spike detection.

    - Provides simple regime mapping and correlation computation from returns.
    - Detects spikes vs regime baselines and escalates via `sentinel_api.set_gate`.
    - Returns suggested allocator compression factor when spike detected.
    """

    REGIME_BASE_MEAN = {
        "risk-on": 0.10,
        "risk-off": 0.60,
        "inflation-shock": 0.50,
    }

    def __init__(self, sentinel_api=None, audit_fn=None, spike_threshold: float = 0.15):
        self.sentinel_api = sentinel_api
        self.audit_fn = audit_fn or (lambda e, p: None)
        self.spike_threshold = spike_threshold

    def compute_correlation_matrix(
        self, returns: Dict[str, List[float]]
    ) -> Tuple[List[str], List[List[float]]]:
        keys = list(returns.keys())
        n = len(keys)
        mat: List[List[float]] = [[0.0] * n for _ in range(n)]
        # compute simple Pearson correlation
        for i in range(n):
            xi = returns[keys[i]]
            for j in range(n):
                yj = returns[keys[j]]
                mat[i][j] = self._pearson(xi, yj)
        return keys, mat

    def _pearson(self, x: List[float], y: List[float]) -> float:
        n = min(len(x), len(y))
        if n < 2:
            return 0.0
        xm = sum(x[:n]) / n
        ym = sum(y[:n]) / n
        cov = sum((x[k] - xm) * (y[k] - ym) for k in range(n))
        varx = sum((x[k] - xm) ** 2 for k in range(n))
        vary = sum((y[k] - ym) ** 2 for k in range(n))
        denom = sqrt(varx * vary)
        if denom == 0:
            return 0.0
        return cov / denom

    def detect_regime(self, market_volatility: float, inflation_signal: float) -> str:
        # simple heuristic mapping
        if inflation_signal > 1.0:
            return "inflation-shock"
        if market_volatility < 0.2:
            return "risk-on"
        return "risk-off"

    def assess_and_escalate(
        self, returns: Dict[str, List[float]], regime: str = "risk-on"
    ) -> Dict[str, object]:
        """Compute correlation, compare vs regime baseline, and escalate if spike.

        Returns report with keys: mean_corr, baseline, delta, compression (<=1.0), escalated(bool).
        """
        keys, mat = self.compute_correlation_matrix(returns)
        mean_corr = _mean_abs_offdiag(mat)
        baseline = self.REGIME_BASE_MEAN.get(regime, 0.2)
        delta = mean_corr - baseline
        escalated = False
        compression = 1.0

        if delta > self.spike_threshold:
            # suggest compressing allocations proportionally
            compression = max(0.1, baseline / mean_corr) if mean_corr > 0 else 1.0
            escalated = True
            reason = f"corr_spike:{regime}:mean={mean_corr:.4f}:baseline={baseline:.4f}:delta={delta:.4f}"
            try:
                if self.sentinel_api is not None:
                    self.sentinel_api.set_gate(2, reason)
            except Exception:
                pass
            self.audit_fn(
                "corr_escalation", {"reason": reason, "compression": compression}
            )

        return {
            "assets": keys,
            "matrix": mat,
            "mean_corr": mean_corr,
            "baseline": baseline,
            "delta": delta,
            "compression": compression,
            "escalated": escalated,
        }
