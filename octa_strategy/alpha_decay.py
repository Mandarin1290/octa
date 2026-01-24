import hashlib
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def canonical_hash(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


@dataclass
class DecayAlert:
    timestamp: str
    signal: str
    regime: Optional[str]
    baseline_corr: float
    recent_corr: float
    drop_fraction: float
    severity: str
    evidence_hash: str = ""


class AlphaDecayDetector:
    """Detect alpha decay and performance drift in a regime-aware manner.

    Methods:
    - add_observation(date, signal, ret, regime)
    - rolling_correlation(signal, regime=None)
    - detect_decay(signal, regime=None)

    Approach summary:
    - Maintain history per signal and per regime.
    - Compute long-term correlation (baseline) and short-term correlation (recent window).
    - If baseline positive and recent drops below ratio threshold => flag decay.
    - Also optionally detect negative slope in recent correlations.
    """

    def __init__(
        self,
        long_window: int = 120,
        short_window: int = 20,
        drop_threshold: float = 0.5,
        min_samples: int = 30,
        escalate_count: int = 2,
    ):
        self.long_window = int(long_window)
        self.short_window = int(short_window)
        self.drop_threshold = float(
            drop_threshold
        )  # fraction of baseline (e.g., 0.5 => recent < 50% baseline)
        self.min_samples = int(min_samples)
        self.escalate_count = int(escalate_count)

        # structure: signal -> list of observations (dicts with date, signal, ret, regime)
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        # counts to escalate
        self._counts: Dict[str, int] = {}
        self.alerts: List[DecayAlert] = []
        self.audit_log: List[Dict[str, Any]] = []

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _record_audit(self, action: str, details: Dict[str, Any]):
        ts = self._now_iso()
        rec = {"ts": ts, "action": action, "details": details}
        rec["evidence_hash"] = canonical_hash(rec)
        self.audit_log.append(rec)

    def add_observation(
        self,
        date: str,
        signal_name: str,
        signal_value: float,
        ret: float,
        regime: Optional[str] = None,
    ):
        rec = {
            "date": date,
            "signal": float(signal_value),
            "ret": float(ret),
            "regime": regime,
        }
        self._history.setdefault(signal_name, []).append(rec)
        # cap history at 10 * long_window
        max_len = max(10 * self.long_window, 1000)
        if len(self._history[signal_name]) > max_len:
            self._history[signal_name] = self._history[signal_name][-max_len:]
        self._record_audit(
            "add_observation",
            {
                "signal_name": signal_name,
                "date": date,
                "signal": signal_value,
                "ret": ret,
                "regime": regime,
            },
        )

    def _corr(self, pairs: List[Dict[str, Any]]) -> Optional[float]:
        if len(pairs) < 2:
            return None
        xs = [p["signal"] for p in pairs]
        ys = [p["ret"] for p in pairs]
        mean_x = statistics.mean(xs)
        mean_y = statistics.mean(ys)
        statistics.pvariance(xs) * len(xs) if len(xs) > 1 else 0.0
        statistics.pvariance(ys) * len(ys) if len(ys) > 1 else 0.0
        # use sample covariance
        cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=False))
        denom = math.sqrt(
            sum((x - mean_x) ** 2 for x in xs) * sum((y - mean_y) ** 2 for y in ys)
        )
        if denom == 0:
            return None
        return cov / denom

    def _select_history(
        self, signal_name: str, regime: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        hist = self._history.get(signal_name, [])
        if regime is None:
            return hist
        return [h for h in hist if h.get("regime") == regime]

    def rolling_correlation(
        self, signal_name: str, regime: Optional[str] = None
    ) -> Dict[str, Any]:
        hist = self._select_history(signal_name, regime)
        total = len(hist)
        if total < self.min_samples:
            return {"baseline": None, "recent": None, "samples": total}
        baseline_window = min(self.long_window, total)
        recent_window = min(self.short_window, total)
        baseline_pairs = hist[-baseline_window:]
        recent_pairs = hist[-recent_window:]
        baseline_corr = self._corr(baseline_pairs)
        recent_corr = self._corr(recent_pairs)
        self._record_audit(
            "rolling_correlation",
            {
                "signal_name": signal_name,
                "regime": regime,
                "baseline_corr": baseline_corr,
                "recent_corr": recent_corr,
                "samples": total,
            },
        )
        return {"baseline": baseline_corr, "recent": recent_corr, "samples": total}

    def detect_decay(
        self, signal_name: str, regime: Optional[str] = None
    ) -> Optional[DecayAlert]:
        corr = self.rolling_correlation(signal_name, regime)
        baseline = corr["baseline"]
        recent = corr["recent"]
        samples = corr["samples"]
        if baseline is None or recent is None or samples < self.min_samples:
            return None
        # Only consider positive baseline (alpha) as meaningful
        if baseline <= 0:
            self._record_audit(
                "detect_decay_skipped_nonpositive_baseline",
                {"signal_name": signal_name, "baseline": baseline},
            )
            return None
        # compute drop fraction
        drop_fraction = 1.0 - (recent / baseline) if baseline != 0 else 0.0
        self._record_audit(
            "detect_decay_eval",
            {
                "signal_name": signal_name,
                "baseline": baseline,
                "recent": recent,
                "drop_fraction": drop_fraction,
            },
        )
        if drop_fraction >= self.drop_threshold:
            cnt = self._counts.get(signal_name, 0) + 1
            self._counts[signal_name] = cnt
            severity = "warning" if cnt < self.escalate_count else "critical"
            ts = self._now_iso()
            alert = DecayAlert(
                timestamp=ts,
                signal=signal_name,
                regime=regime,
                baseline_corr=baseline,
                recent_corr=recent,
                drop_fraction=drop_fraction,
                severity=severity,
            )
            alert.evidence_hash = canonical_hash(
                {
                    "ts": ts,
                    "signal": signal_name,
                    "regime": regime,
                    "baseline": baseline,
                    "recent": recent,
                    "drop_fraction": drop_fraction,
                    "severity": severity,
                }
            )
            self.alerts.append(alert)
            self._record_audit(
                "decay_alert",
                {
                    "signal_name": signal_name,
                    "regime": regime,
                    "baseline": baseline,
                    "recent": recent,
                    "drop_fraction": drop_fraction,
                    "severity": severity,
                },
            )
            return alert
        return None

    def get_alerts(self) -> List[DecayAlert]:
        return list(self.alerts)


# keep a reference to the stateful class so the factory can call it after we
# override the `AlphaDecayDetector` name with the factory for backwards
# compatibility
_AlphaDecayDetectorStateful = AlphaDecayDetector


# Stateless detector API (backwards-compatible): detect_decay over raw returns
@dataclass
class DecayReport:
    decay_score: float
    confidence: float
    changepoint_index: Optional[int]
    baseline_mean: float
    recent_mean: float
    baseline_std: float
    recent_std: float


def _mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _var(xs: List[float], ddof: int = 0) -> float:
    if not xs:
        return 0.0
    m = _mean(xs)
    s = sum((x - m) ** 2 for x in xs)
    denom = len(xs) - ddof
    return s / denom if denom > 0 else 0.0


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _two_sided_z_pvalue(
    mean1: float, mean2: float, var1: float, var2: float, n1: int, n2: int
) -> float:
    denom = math.sqrt((var1 / max(1, n1)) + (var2 / max(1, n2)))
    if denom == 0.0:
        return 1.0
    z = (mean2 - mean1) / denom
    p = 2.0 * (1.0 - _normal_cdf(abs(z)))
    return max(0.0, min(1.0, p))


def _page_hinkley(xs: List[float], delta: float = 0.0) -> Dict[str, Optional[float]]:
    mean_so_far = 0.0
    n = 0
    cumulative = 0.0
    min_cum = 0.0
    max_cum = 0.0
    detect_idx: Optional[int] = None
    for i, x in enumerate(xs):
        n += 1
        mean_so_far += (x - mean_so_far) / n
        d = x - mean_so_far - delta
        cumulative += d
        if cumulative < min_cum:
            min_cum = cumulative
        if cumulative > max_cum:
            max_cum = cumulative
        if detect_idx is None and (max_cum - min_cum) > 1e-8:
            detect_idx = i
    ph_mag = max_cum - min_cum
    return {"ph_mag": ph_mag, "detect_index": detect_idx}


class AlphaDecayDetectorStateless:
    """Stateless alpha decay detector operating on a plain list of returns.

    This implements a deterministic combination of Page-Hinkley and z-test measures.
    """

    def __init__(
        self, baseline_window: int = 120, recent_window: int = 30, ph_delta: float = 0.0
    ):
        self.baseline_window = int(baseline_window)
        self.recent_window = int(recent_window)
        self.ph_delta = float(ph_delta)

    def detect_decay(self, returns: List[float]) -> DecayReport:
        n = len(returns)
        if n == 0:
            return DecayReport(0.0, 0.0, None, 0.0, 0.0, 0.0, 0.0)

        bw = min(self.baseline_window, max(1, n - self.recent_window))
        rw = (
            min(self.recent_window, n - bw)
            if n - bw > 0
            else min(self.recent_window, n)
        )

        baseline = returns[:bw]
        recent = returns[-rw:] if rw > 0 else returns[-1:]

        baseline_mean = _mean(baseline)
        recent_mean = _mean(recent)
        baseline_var = _var(baseline)
        recent_var = _var(recent)
        baseline_std = math.sqrt(baseline_var)
        recent_std = math.sqrt(recent_var)

        ph = _page_hinkley(returns, delta=self.ph_delta)
        ph_mag = float(ph.get("ph_mag") or 0.0)
        ph_idx_raw = ph.get("detect_index")
        ph_idx: Optional[int]
        if ph_idx_raw is None:
            ph_idx = None
        elif isinstance(ph_idx_raw, int):
            ph_idx = ph_idx_raw
        elif isinstance(ph_idx_raw, float):
            ph_idx = int(ph_idx_raw)
        else:
            ph_idx = None

        pval = _two_sided_z_pvalue(
            baseline_mean,
            recent_mean,
            baseline_var,
            recent_var,
            len(baseline),
            len(recent),
        )
        confidence = 1.0 - pval

        eps = 1e-12
        mean_drop = baseline_mean - recent_mean
        norm_drop = mean_drop / (baseline_std + eps)

        drop_score = (
            1.0 / (1.0 + math.exp(-max(-50.0, min(50.0, norm_drop))))
            if not math.isnan(norm_drop)
            else 0.0
        )

        scale = (baseline_std + eps) * math.sqrt(max(1, len(returns)))
        ph_norm = ph_mag / (scale + eps)
        ph_score = math.tanh(ph_norm * 2.0)

        if mean_drop <= 0:
            decay_score = 0.0
            confidence = 0.0
        else:
            decay_score = drop_score * ph_score * confidence

        decay_score = max(0.0, min(1.0, decay_score))
        confidence = max(0.0, min(1.0, confidence))

        return DecayReport(
            decay_score=decay_score,
            confidence=confidence,
            changepoint_index=ph_idx,
            baseline_mean=baseline_mean,
            recent_mean=recent_mean,
            baseline_std=baseline_std,
            recent_std=recent_std,
        )


# Backwards-compatible factory: if user supplies baseline_window param, create stateless detector
def _make_detector_from_kwargs(**kwargs):
    if "baseline_window" in kwargs or "recent_window" in kwargs:
        bw = kwargs.get("baseline_window", kwargs.get("long_window", 120))
        rw = kwargs.get("recent_window", kwargs.get("short_window", 30))
        ph = kwargs.get("ph_delta", 0.0)
        return AlphaDecayDetectorStateless(
            baseline_window=bw, recent_window=rw, ph_delta=ph
        )
    # otherwise create the stateful/regime-aware detector
    return _AlphaDecayDetectorStateful(
        **{
            k: v
            for k, v in kwargs.items()
            if k
            in (
                "long_window",
                "short_window",
                "drop_threshold",
                "min_samples",
                "escalate_count",
            )
        }
    )


# Expose AlphaDecayDetector name as a factory for backwards compatibility
def AlphaDecayDetector_factory(*args, **kwargs):
    # accept positional args mapping to long_window, short_window, drop_threshold, min_samples, escalate_count
    if args and ("baseline_window" in kwargs or "recent_window" in kwargs):
        return _make_detector_from_kwargs(**kwargs)
    if args and not kwargs:
        # assume positional correspond to long_window, short_window, drop_threshold, min_samples, escalate_count
        names = [
            "long_window",
            "short_window",
            "drop_threshold",
            "min_samples",
            "escalate_count",
        ]
        kw = {n: v for n, v in zip(names, args, strict=False)}
        return _make_detector_from_kwargs(**kw)
    return _make_detector_from_kwargs(**kwargs)


# override name in module namespace (kept as runtime factory; typed as Any for backwards compatibility)
AlphaDecayDetector = AlphaDecayDetector_factory  # type: ignore
