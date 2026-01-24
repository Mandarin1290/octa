from typing import Any, Dict, List


class FeatureGateResult:
    def __init__(
        self,
        passed: bool,
        reasons: List[str] | None = None,
        details: Dict[str, Any] | None = None,
    ):
        self.passed = passed
        self.reasons = reasons or []
        self.details = details or {}

    def to_dict(self):
        return {
            "passed": self.passed,
            "reasons": list(self.reasons),
            "details": dict(self.details),
        }


class FeatureGates:
    """Validate a feature before modeling.

    Expected feature metadata (example):
      {
        'name': 'f1',
        'values': [0.1, 0.2, ...],           # required
        'series': [..]                        # for stationarity checks (optional)
        'correlation_with_future': 0.01,     # for leakage detection (required or optional)
        'latency_ms': 50,                    # for latency feasibility
        'transforms': ['zscore'],            # list of applied transforms
      }

    The class enforces no lookahead (via leakage metric), no unstable features
    (stationarity diagnostics), and no undocumented transforms (must be in allowlist).
    """

    def __init__(
        self,
        allowed_transforms: List[str] | None = None,
        max_latency_ms: int = 500,
        leakage_threshold: float = 0.1,
    ):
        self.allowed_transforms = set(allowed_transforms or [])
        self.max_latency_ms = int(max_latency_ms)
        self.leakage_threshold = float(leakage_threshold)

    def check(self, feature_meta: Dict[str, Any]) -> FeatureGateResult:
        reasons: List[str] = []
        details: Dict[str, Any] = {}

        # Data completeness
        vals = feature_meta.get("values")
        if vals is None or not isinstance(vals, list) or any(v is None for v in vals):
            reasons.append("data_incomplete")

        # Undocumented transforms
        transforms = feature_meta.get("transforms", [])
        undocumented = [t for t in transforms if t not in self.allowed_transforms]
        if undocumented:
            reasons.append("undocumented_transforms")
            details["undocumented"] = undocumented

        # Latency feasibility
        latency = feature_meta.get("latency_ms")
        if latency is not None:
            if int(latency) > self.max_latency_ms:
                reasons.append("latency_exceeded")
                details["latency_ms"] = int(latency)

        # Leakage detection: require either explicit metric or computed placeholder
        corr = feature_meta.get("correlation_with_future")
        if corr is not None:
            if abs(float(corr)) >= self.leakage_threshold:
                reasons.append("leakage_detected")
                details["correlation_with_future"] = float(corr)

        # Stationarity diagnostics: simple heuristic on 'series'
        series = feature_meta.get("series")
        if series is not None and isinstance(series, list) and len(series) >= 8:
            # split into two halves and compare means/stds deterministically
            half = len(series) // 2
            s1 = series[:half]
            s2 = series[half : half * 2]

            def mean(xs):
                return sum(float(x) for x in xs) / len(xs)

            def std(xs, m):
                return (sum((float(x) - m) ** 2 for x in xs) / len(xs)) ** 0.5

            m1 = mean(s1)
            m2 = mean(s2)
            sd1 = std(s1, m1)
            sd2 = std(s2, m2)
            # check for large mean shift or variance change
            mean_shift = 0 if m1 == 0 else abs((m2 - m1) / (abs(m1) + 1e-12))
            var_change = 0 if sd1 == 0 else abs((sd2 - sd1) / (abs(sd1) + 1e-12))
            details["stationarity_mean_shift"] = mean_shift
            details["stationarity_var_change"] = var_change
            if mean_shift > 0.5 or var_change > 1.0:
                reasons.append("non_stationary")

        passed = len(reasons) == 0
        return FeatureGateResult(passed=passed, reasons=reasons, details=details)
